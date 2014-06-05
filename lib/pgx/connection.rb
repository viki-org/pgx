require 'pgx/sql'
require 'pg'

module PGx
  class Connection < PG::Connection
    extend PGx::SQL

    attr_accessor :ignore_errors
    alias_method :ignore_errors?, :ignore_errors

    methods.select { |method| method.to_s =~ /^build.*sql$/ }.each do |build_method|
      exec_method = (build_method.to_s.sub(/^build(.*)_sql$/, 'exec\1')).to_sym
      define_method exec_method do |*args|
        query = self.class.send(build_method, *args)
        exec query
      end
    end

    def self.connect connection_hash={ }
      defaults = PGx.database_config
      ignore_errors = connection_hash.delete(:ignore_errors)
      conn = self.new defaults.merge(connection_hash)
      conn.set_notice_receiver { |result| PGx.log.warn result.error_message.strip }
      conn.ignore_errors = ignore_errors

      if block_given?
        begin
          result = yield conn
        rescue
          raise
        ensure
          conn.close
        end
        return result
      end

      conn
    end

    def exec_simple(*args)
      rs = exec *args
      return nil if rs.count < 1
      rs[0][rs[0].keys[0]]
    end

    def exec_and_log(*args)
      begin
        PGx.log.debug get_sql_and_args(args)
        exec *args
      rescue PG::Error
        PGx.log.error "Error executing:#{get_sql_and_args(args)}"
        raise unless ignore_errors?
      end
    end

    def exec_file filename
      file_content = File.read(filename)
      queries = file_content.split(';').map(&:strip).reject { |q| q.blank? }
      queries.map do |q|
        result = exec q
        result = yield result if block_given?
        result
      end
    end

    def get_sql_and_args(args)
      sql_and_args = "\n#{args[0]}"
      sql_and_args << "\n            #{args[1]}" if (args.length > 1 && args[1].is_a?(Array))
      sql_and_args
    end

    def schema_exists? schema_name
      return true if schema_name.to_sym == :public
      exec_simple("SELECT 1 FROM information_schema.schemata WHERE schema_name = $1", [schema_name]) == '1'
    end

    def table_exists? table_name, schema_name
      sql = <<-SQL.strip_heredoc
        SELECT COUNT(1)
        FROM pg_class C LEFT JOIN pg_namespace N ON C.relnamespace = N.oid
        WHERE C.relname = $1
          AND N.nspname = $2 AND C.relkind= 'r';
      SQL
      count = exec_simple(sql, [table_name, schema_name]).to_i
      count > 0 ? true : false
    end

    def index_exists? index_name, schema_name
      sql = "SELECT COUNT(1) FROM pg_indexes WHERE schemaname = $2 AND indexname = $1"
      count = exec_simple(sql, [index_name, schema_name]).to_i
      count > 0 ? true : false
    end

    def fetch_table_names schema_name
      sql = <<-SQL.strip_heredoc
      SELECT C.relname AS table_name
      FROM pg_class C,
           pg_namespace N
      WHERE C.relkind = 'r'
        AND C.relnamespace = N.oid
        AND N.nspname = $1
      ORDER BY 1;
      SQL
      exec(sql, [schema_name]).map { |row| row['table_name'] }
    end
    
    def fetch_schema_names
      sql = <<-SQL.strip_heredoc
      SELECT S.schema_name
      FROM information_schema.schemata S
      WHERE NOT(S.schema_name like 'pg_%')
      ORDER BY 1;
      SQL
      exec(sql).map { |row| row['schema_name'] }
    end

    def fetch_relation_sizes
      sql = <<-SQL.strip_heredoc
      SELECT
        nspname || '.' || relname AS "relation",
        pg_size_pretty(pg_relation_size(C.oid)) AS "size",
        TS.spcname AS "table space"
      FROM pg_class C
      LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      LEFT JOIN pg_tablespace TS ON C.reltablespace = TS.oid
      WHERE nspname NOT IN ('pg_catalog', 'information_schema')
      ORDER BY pg_relation_size(C.oid) DESC;
      SQL
      exec(sql).map { |row| { relation: row["relation"], size: row["size"], tablespace: row["table space"] } }
    end
  
    def fetch_tablespace_names
        sql = <<-SQL.strip_heredoc
        SELECT
          spcname
        FROM
          pg_tablespace;
        SQL
        exec(sql).map { |row| row['spcname'] }
    end
  end
end
