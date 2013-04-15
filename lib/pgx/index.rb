module PGx
  class Index
    attr_accessor :table, :column_names, :name
    attr_writer :primary, :unique

    def self.fetch connection, table, index_name
      index_info = fetch_index_info connection, index_name, table.schema
      column_names = fetch_column_names connection, index_info['oid']
      options = {
        name: index_name,
        unique: index_info['unique'],
        primary: index_info['primary']
      }
      self.new table, column_names, options
    end

    def initialize table, column_names, options = { }
      @table = table
      @column_names = Array column_names
      @name = options[:name]
      @primary = !!options[:primary]
      @unique = !!options[:unique]
      @where = options[:where]

      raise "Not a table: #{table}" if table.nil? || !table.is_a?(PGx::Table)
    end

    def == other_index
      return false unless equivalent_index? other_index
      name == other_index.name
    end

    def name
      base_name = @name || generated_name
      table.temp? ? "temp_#{base_name}" : base_name
    end

    def schema
      table.schema
    end

    def qualified_name
      %Q{"#{schema}"."#{name}"}
    end

    def primary?
      @primary
    end

    def unique?
      @unique || primary?
    end

    def exists?
      match = table.fetch_indexes.detect { |i| i.column_names == column_names }
      !match.nil?
    end

    def equivalent_index? other_index
      return false unless other_index.is_a? self.class
      [:schema, :column_names, :primary?, :unique?].each do |sym|
        return false unless self.send(sym) == other_index.send(sym)
      end
      table.name == other_index.table.name
    end

    def create
      PGx.log.info "Indexing #{table.qualified_name} on #{column_names.join(', ')} (#{name}) #@where"

      if table.connection.index_exists?(name, table.schema)
        PGx.log.info "Index #{name} already exists. Dropping"
        drop
      end

      start_time = Time.now
      options = { column_array: column_names, unique: unique?, schema_name: schema, where: @where }
      table.connection.exec_create_index(name, table.name, options)

      if primary?
        options = { primary_index: name, schema_name: schema }
        table.connection.exec_alter_table(table.name, options)
      end
      PGx.log.info "Indexing completed in #{Time.now - start_time} seconds"
    end

    def drop options = { }
      table.connection.exec_drop_index(name, options.merge(schema_name: schema))
    end

    def rename new_name
      table.connection.exec_alter_index(name, rename_to: new_name, schema_name: schema)
    end

    def to_hash
      {
        table_name: table.name,
        schema: schema,
        columns: column_names,
      }.tap do |hash|
        if primary?
          hash[:primary] = true
        elsif unique?
          hash[:unique] = true
        end
        hash[:name] = @name if @name && @name != generated_name
      end
    end

    protected

    def self.fetch_index_info connection, index_name, schema_name
      sql = <<-SQL.strip_heredoc
      SELECT
          C.oid,
          I.indisunique AS "unique",
          I.indisprimary AS "primary"
      FROM pg_catalog.pg_class C,
           pg_catalog.pg_namespace N,
           pg_catalog.pg_index I
      WHERE C.relname = $2
        AND C.relnamespace = N.oid
        AND I.indexrelid = C.oid
        AND N.nspname = $1;
      SQL
      connection.exec(sql, [schema_name, index_name])[0].tap do |info|
        info['unique'] = info['unique'] != 'f'
        info['primary'] = info['primary'] != 'f'
      end
    end

    def self.fetch_column_names connection, oid
      sql = <<-SQL.strip_heredoc
      SELECT
           pg_catalog.pg_get_indexdef(A.attrelid, A.attnum, TRUE) AS "column_name"
      FROM pg_catalog.pg_attribute A
      WHERE A.attrelid = $1
        AND A.attnum > 0
        AND NOT A.attisdropped
      ORDER BY A.attnum;
      SQL
      connection.exec(sql, [oid]).map { |row| row['column_name'] }
    end

    def generated_name
      "idx_#{table.base_name}_on_#{column_names[0]}".tap do |index|
        column_names[1..-1].each do |column_name|
          index << "_" << (table.columns.index { |column| column[:column_name] == column_name } + 1).to_s
        end
      end
    end

  end
end
