module PGx
  module SQLHelper
    def add_index connection, schema_name, table_name, column_names, options = { }
      table = PGx::Table.fetch(connection, table_name, schema_name)
      index = PGx::Index.new table, column_names, options
      index.create unless index.exists?
    end

    def drop_table connection, schema, table_name, options = { }
      return if options[:check_exists] and !connection.table_exists?(table_name, schema)
      connection.exec_and_log "DROP TABLE #{schema}.#{table_name}"
    end

    def add_column connection, table, column_name, column_type, extra = nil
      connection.exec_and_log "ALTER TABLE #{table.qualified_name} ADD COLUMN #{column_name} #{column_type} #{extra}"
    end

    def create_schema connection, schema_name
      connection.exec_and_log "CREATE SCHEMA #{schema_name}" unless connection.schema_exists?(schema_name)
    end

  end
end
