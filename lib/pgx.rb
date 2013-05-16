require 'active_support/all'
require 'pgx/initializers'
require 'pgx/sql'
require 'pgx/connection'
require 'pgx/index'
require 'pgx/table'
require 'pgx/output'

class NullLogger
  def info msg
  end

  def debug msg
  end

  def error msg
  end

  def warn msg
  end
end


module PGx
  class << self
    attr_accessor :table_path, :default_database_config, :log
  end

  self.log = NullLogger.new

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

