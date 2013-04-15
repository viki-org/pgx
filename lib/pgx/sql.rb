module PGx
  module SQL
    ## Options:
    #   schema_name:  [string] schema name
    #   column_array: [array] array of column info hashes
    #   like:         [qualified table name] the table to clone
    def build_create_table_sql(table_name, options = { })
      return nil if table_name.nil? || table_name.empty?
      table_name = get_qualified_relation_name(table_name, options[:schema_name])

      td = { column_array: [] }.merge options

      sql = "CREATE #{"UNLOGGED " if options[:unlogged]}TABLE #{table_name}"

      if options.has_key? :like
        sql << " (LIKE #{options[:like]})"
      else
        sql << "\n("
        sql << td[:column_array].map { |cd| build_column_sql cd }.join(",\n ")
        sql << "\n )"
      end

      sql << ";"
    end

    def build_drop_table_sql(table_name, options = { })
      return nil if table_name.nil? || table_name.empty?
      table_name = get_qualified_relation_name(table_name, options[:schema_name])
      sql = "DROP TABLE"
      sql << " IF EXISTS" if options[:check_exists]
      sql << " #{ table_name };"
    end

    def build_alter_table_sql(table_name, options = { })
      return nil if table_name.nil? || table_name.empty?
      return nil if options.nil? || options.empty?

      table_name = get_qualified_relation_name(table_name, options[:schema_name])

      sql = "ALTER TABLE #{table_name}"

      if options.has_key? :rename_to
        sql << %Q{ RENAME TO "#{options[:rename_to]}"}
      elsif options.has_key? :primary_index
        sql << %Q{ ADD PRIMARY KEY USING INDEX "#{options[:primary_index]}"}
      elsif options.has_key? :new_schema
        sql << %Q{ SET SCHEMA "#{options[:new_schema]}"}
      end

      sql << ";"
    end

    def build_create_index_sql(index_name, table_name, options = { })
      return nil if table_name.nil? || table_name.empty?
      return nil if index_name.nil? || index_name.empty?

      table_name = get_qualified_relation_name(table_name, options[:schema_name])

      where_clause = " WHERE #{options[:where]}" unless options[:where].blank?

      sql = "CREATE "
      sql << "UNIQUE " if options[:unique]
      sql << %Q{INDEX "#{index_name}" ON #{ table_name } (}
      sql << options[:column_array].map { |c| c.match(/[A-Z]+/) ? %Q{"#{c}"} : c }.join(", ") if options.has_key? :column_array
      sql << ")#{where_clause};"
    end

    def build_alter_index_sql(index_name, options = { })
      return nil if index_name.nil? || index_name.empty?
      return nil if options.nil? || options.empty?

      index_name = get_qualified_relation_name(index_name, options[:schema_name])

      sql = "ALTER INDEX #{index_name}"
      sql << %Q{ RENAME TO "#{options[:rename_to]}"} if options.has_key? :rename_to
      sql << ";"
    end

    def build_drop_index_sql(index_name, options = { })
      return nil if index_name.nil? || index_name.empty?
      index_name = get_qualified_relation_name(index_name, options[:schema_name])
      sql = "DROP INDEX"
      sql << " IF EXISTS" if options[:check_exists]
      sql << " #{index_name};"
    end

    def build_insert_into_pg_sql(table_name, options)
      insert_into(table_name, options) do |column_array|
        values = (1..column_array.length).map { |i| "$#{i}" }.join(", ")
        "\n     VALUES (#{values})"
      end
    end

    def build_update_pg_sql(table_name, options)
      return nil if table_name.nil? || table_name.empty?
      return nil if options.nil? || options.empty?
      return nil unless options.has_key? :column_array

      table_name = get_qualified_relation_name(table_name, options[:schema_name])
      column_array = options[:column_array]
      where_clause = options[:where_clause]
      values = (1..column_array.length).map { |i| "$#{i}" }.join(", ")

      sql = "UPDATE #{ table_name }"
      sql << "\n  SET ("
      sql << column_array.map { |c| %Q{"#{c}"} }.join(", ")
      sql << ")"
      sql << "\n    = (#{values})"
      sql << "\n  WHERE #{where_clause}" unless where_clause.nil?
      sql << ";"
    end

    def build_insert_into_select_pg_sql(table_name, select_string, options)
      insert_into(table_name, options) do
        "\nSELECT\n    #{select_string}"
      end
    end

    def build_column_sql(column_info)
      column_info = {
        data_type: "VARCHAR(255)",
        constraints: [],
        is_nullable: false,
        column_default: nil
      }.merge column_info

      name = column_info.delete(:column_name)
      type = column_info.delete(:data_type)
      constraints = column_info.delete(:constraints)

      return nil if name.nil? || name.empty?

      column_info.each do |key, value|
        case key
          when :is_nullable
            constraints << "NOT NULL" unless value
          when :column_default
            constraints << "DEFAULT #{value}" unless value.nil?
        end
      end

      ([%Q{"#{name}"}, type] + constraints).join(" ")
    end

    def get_qualified_relation_name(relation_name, schema_name = nil)
      schema_name.nil? ? %Q{"#{relation_name}"} : %Q{"#{schema_name}"."#{relation_name}"}
    end

    private

    def insert_into(table_name, options, &block)
      return nil if table_name.nil? || table_name.empty?
      return nil if options.nil? || options.empty?

      table_name = get_qualified_relation_name(table_name, options[:schema_name])

      sql = "INSERT INTO #{ table_name }"

      if options.has_key? :column_array
        column_array = options[:column_array]
        sql << "\n            ("
        sql << column_array.map { |c| %Q{"#{c}"} }.join(", ")
        sql << ")"

        sql << block.call(column_array)
      end

      sql << ";"
    end
  end
end
