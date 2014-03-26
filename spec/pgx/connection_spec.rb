require 'spec_helper'

describe PGx::Connection do

  describe '.connect' do
    it "should return an open instance of connection" do
      connection = PGx::Connection.connect
      connection.should be_instance_of PGx::Connection
      connection.should_not be_finished
      connection.close
    end

    context "when given a block" do
      it "yields with an open connection" do
        did_yield = false
        PGx::Connection.connect do |connection|
          did_yield = true
          connection.should_not be_finished
        end
        did_yield.should be_true
      end

      it "closes the connection upon exiting the block" do
        @conn = nil
        PGx::Connection.connect { |connection| @conn = connection }
        @conn.should be_finished
      end

      it "should return the return value of the block" do
        result = PGx::Connection.connect { "foo" }
        result.should == "foo"
      end

      context "when an exception is raised in the block" do
        it "makes sure the connection is closed, and re-raises the exception" do
          conn = nil
          exception = Exception.new

          expect do
            described_class.connect do |connection|
              conn = connection
              raise exception
            end
          end.to raise_error exception

          conn.should be_finished
        end
      end
    end

    context "when give options" do
      let(:connection) { described_class.connect(ignore_errors: true) }

      after { connection.close }

      it { connection.ignore_errors?.should be_true }
    end
  end

  describe '#exec_simple', with_schema: TEST_SCHEMA_NAME do
    let(:connection) { described_class.connect }
    let(:table) { table = FactoryGirl.build :table, :with_columns, connection: connection }

    before do
      table.create
      table.insert [table.column_names.zip(['foo', 1])]
      table.insert [table.column_names.zip(['bar', 2])]
    end

    after { connection.close }

    it "should return the value of the first column, n the first row of the result" do
      connection.exec_simple("SELECT * FROM #{table.qualified_name};").should == 'foo'
    end

    it "should return nil if there are no result rows" do
      connection.exec_simple("SELECT * FROM #{table.qualified_name} WHERE wei_column = 'baz';").should be_nil
    end
  end

  describe 'exec methods' do
    it "should call the corresponding build method and then exec and log the result" do
      build_methods = PGx::SQL.instance_methods.select { |method| method.to_s =~ /^build.*sql$/ }

      described_class.connect do |connection|
        build_methods.each do |build_method|
          arg = double 'foo'
          query = double 'query'

          exec_method = build_method.to_s.sub(/^build_(.*)_sql$/, 'exec_\1')
          described_class.should_receive(build_method).with(arg).and_return(query)
          connection.should_receive(:exec).with(query)
          connection.send exec_method, arg
        end
      end
    end
  end

  describe "#exec_and_log" do
    context "when the query causes an error" do
      it "logs and re-raises PG::Error" do
        PGx.log.should_receive(:error).with("Error executing:\neep\nopp\n            [:ork, :ah_ah]")

        expect do
          described_class.connect do |connection|
            connection.stub(:exec).and_raise PG::Error
            connection.exec_and_log "eep\nopp", [:ork, :ah_ah]
          end
        end.to raise_error PG::Error
      end

      context "when ignore_errors is set to true" do
        it "swallows all exceptions" do
          expect do
            described_class.connect ignore_errors: true do |connection|
              connection.stub(:exec).and_raise PG::Error
              connection.exec_and_log "eep\nopp", [:ork, :ah_ah]
            end
          end.not_to raise_error
        end
      end

    end
  end

  describe "#exec_file" do
    let(:connection) { PGx::Connection.connect }
    let(:fake_connection) { double(:connection).as_null_object }
    let(:filename) { "some_file.sql" }
    let(:file_content) { 'query' }

    before do
      File.stub(:read).with(filename).and_return(file_content)
    end

    it "execute the query specified in the file" do
      result = double(:result)
      connection.should_receive(:exec).with(file_content).and_return(result)

      results = connection.exec_file filename
      results.should == [result]
    end

    context 'when called with a block' do
      it "yields with the result" do
        result = double(:result)
        connection.should_receive(:exec).with(file_content).and_return(result)
        expect { |b| connection.exec_file filename, &b }.to yield_with_args(result)
      end

      it "returns the result of the block" do
        processed_result = double(:processed_result)
        result = double(:result, some_method: processed_result)
        connection.should_receive(:exec).with(file_content).and_return(result)
        results = connection.exec_file(filename) { |r| r.some_method }
        results.should == [processed_result]
      end
    end

    context "when the input contains queries separated by a semicolon" do
      let(:file_content) { "query 1;\nquery 2;\n" }

      it "executes each query, printing out each result" do
        result1 = double(:result)
        result2 = double(:result)

        connection.should_receive(:exec).with('query 1').and_return(result1)
        connection.should_receive(:exec).with('query 2').and_return(result2)

        results = connection.exec_file filename
        results.should == [result1, result2]
      end
    end

  end

  describe '#schema_exists?' do
    it 'returns true when the schema exists', with_schema: TEST_SCHEMA_NAME do
      described_class.connect do |connection|
        connection.schema_exists?(TEST_SCHEMA_NAME).should be_true
      end
    end

    it 'returns false when the schema does not exist' do
      described_class.connect do |connection|
        connection.schema_exists?(:huy_schema).should be_false
      end
    end
  end

  describe '#table_exists', with_schema: TEST_SCHEMA_NAME do
    before do
      described_class.connect do |connection|
        PGx::Table.new(:foo, schema: TEST_SCHEMA_NAME, connection: connection).create
      end
    end

    it "return true if a table exists" do
      described_class.connect do |connection|
        connection.table_exists?(:foo, TEST_SCHEMA_NAME).should == true
      end
    end

    it "return false if a table does not exist" do
      described_class.connect do |connection|
        connection.table_exists?(:bar, TEST_SCHEMA_NAME).should == false
      end
    end
  end

  describe '#index_exists', with_schema: TEST_SCHEMA_NAME do
    let(:columns) { [ {column_name: 'foo' }] }
    let(:indexes) { [ {name: 'foo_index', columns: %w(foo) }] }
    before do
      described_class.connect do |connection|
        table = PGx::Table.new(:foo, columns: columns, indexes: indexes, schema: TEST_SCHEMA_NAME, connection: connection)
        table.create
        table.create_indexes
      end
    end

    it "return true if index exists" do
      described_class.connect do |connection|
        connection.index_exists?(:foo_index, TEST_SCHEMA_NAME).should == true
      end
    end

    it "return false if index does not exist" do
      described_class.connect do |connection|
        connection.index_exists?(:bar_index, TEST_SCHEMA_NAME).should == false
      end
    end
  end

  describe '#fetch_table_names' do
    it "should return an array of the tables in the schema" do
      described_class.connect do |connection|
        PGx::Table.new(:foo, schema: TEST_SCHEMA_NAME, connection: connection).create
        PGx::Table.new(:bar, schema: TEST_SCHEMA_NAME, connection: connection).create
        connection.fetch_table_names(TEST_SCHEMA_NAME).should == %w(bar foo)
      end
    end
  end
  
  describe '#fetch_schema_names' do
    it "should return an array of schema names" do
      described_class.connect do |connection|
        connection.fetch_schema_names.should include TEST_SCHEMA_NAME
      end
    end
    it "should exclude postgresql schemas" do
      described_class.connect do |connection|
        connection.fetch_schema_names.select {|schema_name| /^pg_/ =~ schema_name}.should be_empty
      end
    end
  end
end

