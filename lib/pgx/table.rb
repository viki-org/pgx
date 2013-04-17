require 'pgx/sql_helper'

module PGx
  class Table
    DEFAULT_SCHEMA = "reporting"
    include ::PGx::SQLHelper

    attr_accessor :schema, :columns, :indexes, :connection, :unlogged
    attr_reader :base_name

    def self.load table_name, options = { }
      path = options[:path] || PGx.table_path
      pathname = Pathname.new File.join(path, "#{table_name}.json")
      table_spec = JSON.load(pathname)
      table_spec.recursive_symbolize_keys!

      table_name = table_spec.delete(:table_name) || table_name

      table_options = {
        schema: table_spec.delete(:schema_name),
        columns: table_spec.delete(:columns),
        indexes: table_spec.delete(:indexes),
        unlogged: table_spec.delete(:unlogged)._?(true)
      }.merge(options)
      self.new table_name, table_options
    end

    # Currently can't check if the table is UNLOGGED
    def self.fetch connection, table_name, schema_name = DEFAULT_SCHEMA

      unless connection.schema_exists? schema_name
        PGx.log.error "Schema #{schema_name} does not exist"
        return nil
      end

      table = self.new(table_name,
                       schema: schema_name,
                       columns: fetch_columns(connection, table_name, schema_name),
                       connection: connection).tap do |table|
        table.indexes = table.fetch_indexes
      end

      unless table.exists?
        PGx.log.error "Table #{table.qualified_name} does not exist"
        return nil
      end

      table
    end

    # Doesn't work for tables that does not belong to the user
    # TODO: Query pg_catalog instead
    def self.fetch_columns connection, table_name, schema_name
      query = <<-SQL.strip_heredoc
    SELECT
        column_name,
        is_nullable,
        CASE WHEN data_type = 'character' THEN 'CHAR(' || character_maximum_length || ')'
             WHEN data_type = 'character varying' THEN 'VARCHAR(' || COALESCE(character_maximum_length, 255) || ')'
             WHEN data_type = 'numeric' THEN 'NUMERIC(' || COALESCE(numeric_precision, 50) || ',' || COALESCE(numeric_scale, 20) || ')'
             WHEN data_type = 'integer' THEN 'INT'
             WHEN data_type = 'ARRAY' THEN 'VARCHAR(255)[]'
             ELSE UPPER(data_type) END AS "data_type",
        column_default
    FROM information_schema.COLUMNS
    WHERE table_schema = $2
      AND table_name = $1
    ORDER BY ORDINAL_POSITION;
      SQL

      rs = connection.exec query, [table_name, schema_name]
      rs.map do |t|
        t['is_nullable'] = t['is_nullable'] == 'YES'
        t.symbolize_keys
      end
    end

    private_class_method :fetch_columns

    def self.inject_raw_columns column_array
      column_array.map do |c|
        [c].tap do |columns|
          raw_info = c[:pg_raw]
          if raw_info
            raw_column = { column_name: "#{c[:column_name]}_raw" }
            if Hash === raw_info && raw_info.has_key?(:data_type)
              [:data_type, :is_nullable].each { |k| raw_column[k] = raw_info[k] }
            end
            columns << raw_column
          end
        end
      end.flatten
    end

    def initialize base_name, options = { }
      @base_name = base_name
      @columns = options[:columns] || []
      @schema = options[:schema] || DEFAULT_SCHEMA
      @temp = !(!options[:temp])
      @connection = options[:connection]
      @unlogged = options[:unlogged]._?(true)

      self.indexes = options[:indexes] || []
    end

    def temp?
      @temp
    end

    def unlogged?
      @unlogged
    end

    def qualified_name
      %Q{"#{schema}"."#{name}"}
    end

    def name
      temp? ? temp_name : base_name
    end

    def connection
      raise "table does not have an open connection!" if @connection.nil?
      @connection
    end

    def ==(other_table)
      return false unless other_table.is_a? self.class
      variables_to_match = self.instance_variables.reject { |v| v == :@connection }
      variables_to_match.each do |sym|
        return false unless self.instance_variable_get(sym) == other_table.instance_variable_get(sym)
      end
    end

    def column_names
      columns.map { |c| c[:column_name] }
    end

    def column_names_with_raw_columns
      self.class.inject_raw_columns(columns).map { |c| c[:column_name] }
    end

    def copy_from table
      table.instance_variables.each do |x|
        next if x == :@indexes
        instance_variable_set x, table.instance_variable_get(x)
      end

      @indexes = table.indexes.map do |index|
        index.dup.tap { |new_index| new_index.table = self }
      end

      self
    end

    def get_temp_table
      PGx::Table.allocate.tap do |temp|
        temp.copy_from self
        temp.instance_variable_set(:@temp, true)
      end
    end

    def with_temp_table
      yield get_temp_table
    end

    def create options = { }
      self.drop check_exists: true if options[:force]
      connection.exec_create_table name, options.merge(unlogged: unlogged, schema_name: schema, column_array: self.class.inject_raw_columns(columns))
    end

    def clone_rename new_name
      PGx::Table.allocate.tap do |t|
        t.copy_from self
        t.instance_variable_set(:@base_name, new_name)
        t.indexes.each { |index| index.name = nil }
      end
    end

    def drop options = { }
      if options[:check_exists]
        return unless self.exists?
      end
      connection.exec_drop_table(name, options.merge({ schema_name: schema }))
    end

    def exists?
      result = connection.exec "SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = '#{schema}' AND tablename = '#{name}'"
      result.count == 1
    end

    def insert rows
      return if rows.empty?

      rows.each do |row|
        columns = row.map { |pair| pair[0] }
        values = row.map { |pair| pair[1] }

        sql = connection.class.build_insert_into_pg_sql(name, { schema_name: schema, column_array: columns })
        connection.exec_and_log sql, values
      end
    end

    def insert_batch columns, rows, batch_size = 200
      return if rows.empty?
      sql = connection.class.build_insert_into_pg_sql(name, { schema_name: schema, column_array: columns })

      rows.in_groups_of(batch_size, false).each do |row_group|
        connection.transaction do
          row_group.each { |row| connection.exec_and_log(sql, row) }
        end
      end
    end

    def insert_select select_string, options = { }
      options[:column_array] = options.delete(:column_names) || column_names_with_raw_columns
      options[:schema_name] = schema
      args = options.delete(:arguments) || []
      sql = PGx::Connection.build_insert_into_select_pg_sql name, select_string, options
      connection.exec_and_log sql, args
    end

    def update rows, options={ }
      rows.each do |row|
        columns = row.map { |pair| pair[0] }
        values = row.map { |pair| pair[1] }

        sql = connection.class.build_update_pg_sql(name, { schema_name: schema, column_array: columns, where_clause: options[:where_clause] })
        connection.exec_and_log sql, values
      end
    end

    def indexes= index_array
      @indexes = (index_array || []).map do |index_desc|
        case index_desc
          when PGx::Index
            index_desc
          when Hash
            options = index_desc.dup
            column_names = options.delete(:columns)
            PGx::Index.new self, column_names, options
          else
            raise "Not an index descriptor: #{index_desc}"
        end
      end
    end

    def create_primary_index
      primary_index = indexes.select(&:primary?).first
      primary_index.create unless primary_index.nil?
    end

    def create_indexes
      current_indexes = fetch_indexes
      indexes.each do |index|
        if current_indexes.select { |current_index| current_index.equivalent_index? index }.empty?
          index.create
          current_indexes << index
        end
      end
    end

    def fetch_index_names
      query = <<-SQL.strip_heredoc
        SELECT
            C.relname AS "index_name"
        FROM pg_catalog.pg_class C,
             pg_catalog.pg_namespace N,
             pg_catalog.pg_index I,
             pg_catalog.pg_class C2
        WHERE C.relkind IN ( 'i', '' )
          AND N.oid = C.relnamespace
          AND N.nspname = $2
          AND I.indexrelid = C.oid
          AND C2.oid = I.indrelid
          AND C2.relname = $1;
      SQL
      connection.exec(query, [name, schema]).map { |row| row['index_name'] }
    end

    def select *args
      options = if args.last.is_a? Hash
        args.pop.symbolize_keys
      else
        Hash.new
      end
      where_clause = args.delete_at(0)

      sql = "SELECT "
      sql << (options.has_key?(:columns) ? Array(options[:columns]).join(', ') : '*')
      sql << " FROM #{qualified_name}"
      sql << " WHERE #{where_clause}" unless where_clause.nil?
      sql << " ORDER BY #{options[:order]}" unless options[:order].nil?
      sql << " GROUP BY #{options[:group]}" unless options[:group].nil?
      sql << " HAVING #{options[:having]}" unless options[:having].nil?
      sql << ';'
      connection.exec_and_log sql, args
    end

    def select_simple column, *args
      options = if args.last.is_a? Hash
        args.pop
      else
        Hash.new
      end
      rs = select *args, options.merge(columns: column)
      return nil if rs.count == 0
      rs[0][rs[0].keys[0]]
    end

    def fetch_indexes
      fetch_index_names.map do |index_name|
        PGx::Index.fetch connection, self, index_name
      end
    end

    def with_connection(options = { })
      PGx::Connection.connect(options) do |connection|
        self.connection = connection
        result = yield self, connection
        self.connection = nil
        result
      end
    end

    def schema_hotswap new_schema_name = nil
      new_schema_name ||= "temp_#{schema}"
      connection.transaction do
        create_schema connection, new_schema_name
        drop_table connection, new_schema_name, name, :check_exists => true
        connection.exec_and_log "ALTER TABLE #{qualified_name} SET SCHEMA #{new_schema_name}"
      end
    end

    def hotswap
      temp_table = get_temp_table
      create_like_temp_table
      connection.transaction {
        drop
        temp_table.indexes.zip(indexes).each { |temp_index, index| temp_index.rename index.name }
        temp_table.rename_temp_table
        rename_temp_sequences
      }
    end

    def hotswap_without_schema options = { }
      temp_table = get_temp_table
      create_like_temp_table
      connection.transaction {
        drop
        temp_table.indexes = temp_table.fetch_indexes
        temp_table.instance_variable_set(:@temp, false)
        temp_table.indexes.each { |index| index.rename index.name.gsub(/temp_/, '') }
        temp_table.instance_variable_set(:@temp, true)
        temp_table.rename_temp_table
        rename_temp_sequences unless options[:skip_sequences]
      }
    end

    def insert_through_temp_table rows
      do_through_temp_table { |temp_table| temp_table.insert rows }
    end

    def append_through_temp_table rows
      do_through_temp_table do |temp_table|
        temp_table.insert_select "* FROM #{qualified_name}" if exists?
        temp_table.insert rows
      end
    end

    def to_hash
      {
        table_name: @base_name,
        schema: @schema,
        columns: @columns,
        indexes: @indexes.map { |index| index.to_hash.slice(:columns, :name, :primary, :unique) },
        unlogged: @unlogged,
        temp: @temp
      }
    end

    def to_ruby_string additional_keys = []
      return '' if columns.empty?

      column_keys = [:column_name, :is_nullable, :data_type, :column_default]
      indexes_array = indexes.map &:to_hash
      index_keys = [:columns, :name, :primary, :unique]

      output = PGx::Output.new
      output.<<('{').newline.shift
      output.<<('columns: ').append_hash_array(columns, column_keys, additional_keys: additional_keys)
      output.<<(',').newline
      output.<<('indexes: ').append_hash_array(indexes_array, index_keys)
      output.newline.unshift
      output.<<('}').newline
    end

    def to_sql
      PGx::Output.new.tap do |output|
        output << PGx::Connection.build_create_table_sql(name, schema_name: schema, column_array: columns)
        output.newline

        indexes.each do |index|
          output << PGx::Connection.build_create_index_sql(
            index.name,
            self.name,
            column_array: index.column_names,
            unique: index.unique?,
            schema_name: schema)
          output.newline

          if index.primary?
            output << PGx::Connection.build_alter_table_sql(
              self.name,
              primary_index: index.name,
              schema_name: schema)
            output.newline
          end
        end
      end
    end

    def do_through_temp_table options = { }
      raise "#{self} is a temp table! :(" if temp?

      temp_table = get_temp_table

      temp_table.drop check_exists: true
      temp_table.create

      yield temp_table

      temp_table.create_indexes unless options[:skip_indexes]

      temp_table.vacuum_analyze

      hotswap
    end

    def remove_sequences
      columns.each { |c| c.delete(:column_default) if c[:column_default] =~ /nextval.*::regclass/ }
    end

    def vacuum_analyze
      connection.exec_and_log "VACUUM ANALYZE #{qualified_name}"
    end

    def prepare_insert_ignore_duplicate(connection, pk_fields)
      rule_name = "#{self.name}_on_duplicate_ignore"
      new_pk_fields = pk_fields.map { |f| "NEW.#{f}" }

      connection.exec <<-SQL.strip_heredoc
      CREATE RULE "#{rule_name}" AS ON INSERT TO #{self.qualified_name}
        WHERE EXISTS( SELECT 1 FROM #{self.qualified_name}
                      WHERE (#{pk_fields.join(", ")}) = ( #{new_pk_fields.join(", ")} )
                    )
          DO INSTEAD NOTHING;
      SQL

      yield

      connection.exec "DROP RULE #{rule_name} ON #{self.qualified_name}"
    end

    def create_like_temp_table
      raise "#{self} is a temp table! :(" if temp?
      connection.exec_create_table(name, schema_name: schema, like: get_temp_table.qualified_name) unless exists?
    end

    def self.qualified_name_for(schema, table)
      %Q{"#{schema}"."#{table}"}
    end

    protected

    def rename_temp_table
      raise "#{self} is not a temp table! :(" unless temp?

      connection.exec_alter_table name, rename_to: base_name, schema_name: schema
    end

    def rename_temp_sequences
      temp_sequence_names.each do |sequence|
        sequence_name_without_schema = sequence.sub("#{schema}.", '')
        new_sequence_name = sequence_name_without_schema.sub(temp_name, base_name)
        connection.exec_alter_table sequence_name_without_schema, rename_to: new_sequence_name, schema_name: schema
      end
    end

    def temp_sequence_names
      sql_result = connection.exec <<-SQL.strip_heredoc
      SELECT
        PG_GET_SERIAL_SEQUENCE('#{qualified_name}', column_name) AS sequence
      FROM information_schema.COLUMNS
      WHERE table_schema = '#{schema}'
        AND table_name = '#{base_name}'
        AND PG_GET_SERIAL_SEQUENCE('#{qualified_name}', column_name) IS NOT NULL
        AND PG_GET_SERIAL_SEQUENCE('#{qualified_name}', column_name) LIKE '#{schema}.#{temp_name}%';
      SQL

      sql_result.entries.map { |e| e['sequence'] }
    end

    def temp_name
      "temp_#{base_name}"
    end

  end
end
