require 'rubygems'
require 'bundler/setup'
require 'pgx'

TEST_SCHEMA_NAME = "test_schema"

Dir[File.join(File.dirname(__FILE__), "support/**/*.rb")].each { |f| require f }
Dir[File.join(File.dirname(__FILE__), "factories/**/*.rb")].each { |f| require f }

RSpec.configure do |c|
  c.order = "random"

  c.around(:each) do |example|
    schema_names = [TEST_SCHEMA_NAME.to_sym]

    PGx::Connection.connect do |connection|
      schema_names.each do |schema_name|
        connection.exec "CREATE SCHEMA #{schema_name}" unless connection.schema_exists?(schema_name)
      end

      begin
        example.run
      rescue Exception
        raise
      ensure
        schema_names.each { |schema_name| connection.exec "DROP SCHEMA #{schema_name} CASCADE" }
      end
    end
  end
end

PGx.configure do |config|
  config.table_path = "/"

  config.default_database_config = {
    :host => "localhost",
    :dbname => "pgx_test",
    :password => "",
    :user => "postgres"
  }

end

