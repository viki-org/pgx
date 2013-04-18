require 'rubygems'
require 'bundler/setup'
require 'pgx'

TEST_SCHEMA_NAME = "reporting"

Dir[File.join(File.dirname(__FILE__), "support/**/*.rb")].each { |f| require f }
Dir[File.join(File.dirname(__FILE__), "factories/**/*.rb")].each { |f| require f }

RSpec.configure do |c|
  c.order = "random"

  c.around(:each) do |example|
    PGx::Connection.connect do |connection|
      %w(reporting temp_reporting).each { |schema_name| connection.exec "DROP SCHEMA IF EXISTS #{schema_name} CASCADE" }
      connection.exec "CREATE SCHEMA reporting;"
      example.run
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
