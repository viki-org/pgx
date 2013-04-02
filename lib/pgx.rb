require 'log4r'

require 'active_support/all'
require 'pgx/initializers'
require 'pgx/sql'
require 'pgx/connection'
require 'pgx/index'
require 'pgx/table'
require 'pgx/output'

module PGx
  VERSION = "0.0.1"

  LOG = Log4r::Logger.new 'pgx'
  Log4r::Outputter.stdout.formatter = Log4r::PatternFormatter.new(:pattern => "[%l] %d: %m", :date_method => :utc)

  def self.log
    LOG
  end

  class << self
    attr_accessor :table_path, :default_database_config
  end

  def self.configure(&block)
    config = Configurator.new
    block.call config
    @table_path = config.table_path
    @default_database_config = config.default_database_config
    nil
  end

  class Configurator
    attr_accessor :table_path, :default_database_config

    def initialize
    end
  end
end
