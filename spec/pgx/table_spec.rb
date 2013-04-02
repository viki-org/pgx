require 'spec_helper'

describe PGx::Table do
  subject { PGx::Table.new base_name }

  let(:base_name) { 'diego_table' }
  let(:schema) { PGx::Table::DEFAULT_SCHEMA }

  let(:connection) { PGx::Connection.connect }
  after { connection.close }

  it { should be }
  its(:base_name) { should == base_name }
  its(:name) { should == base_name }
  its(:schema) { should == schema }
  its(:qualified_name) { should == %Q{"#{schema}"."#{base_name}"} }
  its(:columns) { should == [] }
  its(:indexes) { should == [] }
  it { should_not be_temp }

  context "options" do
    subject { PGx::Table.new base_name, options }

    describe ':schema' do
      let(:options) { { schema: schema } }
      let(:schema) { 'wei_schema' }

      its(:schema) { should == schema }
      its(:qualified_name) { should == %Q{"#{schema}"."#{base_name}"} }
    end

    describe ':temp' do
      let(:options) { { temp: true } }

      its(:base_name) { should == base_name }
      its(:name) { should == "temp_#{base_name}" }
      its(:qualified_name) { should == %Q{"#{schema}"."temp_#{base_name}"} }
      it { should be_temp }
    end

    describe ':unlogged' do
      let(:options) { { unlogged: true } }

      its(:unlogged) { should == true }
      it { should be_unlogged }
    end

    describe ':columns' do
      let(:options) { { columns: columns } }
      let(:columns) { [{ column_name: 'foo' }, { column_name: 'bar' }] }

      its(:columns) { should == columns }
    end

    describe ':indexes' do
      let(:options) { { columns: columns, indexes: [{ columns: %w(foo) }] } }
      let(:columns) { [{ column_name: 'foo' }, { column_name: 'bar' }] }

      it "should be parsed them into Index objects" do
        subject.indexes.should == [PGx::Index.new(subject, %w(foo))]
      end
    end

    describe ':connection' do
      let(:options) { { connection: connection } }
      its(:connection) { should == connection }
    end
  end

  describe '.load' do
    subject { PGx::Table.load table_name }

    let(:table_name) { 'test_table' }
    let(:table_json) { <<-JSON.strip_heredoc
      {
         "table_name":"test_table",
         "schema_name":"test_schema",
         "columns":[
            { "column_name":"id",  "data_type":"SMALLINT", "api_column_path":[ "result", "id" ] },
            { "column_name":"foo", "is_nullable":true,     "api_column_path":[ "result", "foo" ] }
         ],
         "indexes":[
            { "columns":[ "id" ],       "primary":true },
            { "columns":[ "id", "foo" ] }
         ],
         "unlogged": true
      }
    JSON
    }

    before do
      table_filepath = Pathname.new '/test_table.json'
      JSON.stub(:load).with(table_filepath).and_return(JSON.parse table_json)
    end

    it "loads the table configuration from catalog/table" do
      subject.name.should == table_name
      subject.schema.should == 'test_schema'
      subject.column_names.should == %w(id foo)
      subject.columns[0][:data_type].should == 'SMALLINT'
      subject.columns[1][:api_column_path].should == %w(result foo)
      subject.indexes[1].column_names.should == %w(id foo)
      subject.should be_unlogged
    end

    context "when given options" do
      let(:options) { { } }
      subject { PGx::Table.load table_name, options }

      describe "schema" do
        let(:options) { { schema: "override_schema" } }

        it "should override the values in the config file" do
          subject.schema.should == 'override_schema'
        end
      end

      describe "path" do
        let(:options) { { path: "some/other/path/to" } }

        it "loads the file from the specified path" do
          table_file = Pathname.new 'some/other/path/to/test_table.json'
          JSON.should_receive(:load).with(table_file).and_return(JSON.parse table_json)
          subject
        end
      end

    end
  end

  describe ".fetch", with_schema: TEST_SCHEMA_NAME do
    subject { PGx::Table.fetch(connection, table.name, table.schema) }

    let(:table) do
      PGx::Table.new "diego_table",
                           columns: columns,
                           schema: TEST_SCHEMA_NAME,
                           connection: connection
    end
    let(:columns) do
      [
        { column_name: 'row_id', is_nullable: false, data_type: 'INT', column_default: nil },
        { column_name: 'char_col', is_nullable: true, data_type: 'CHAR(20)', column_default: nil },
        { column_name: 'varchar_col', is_nullable: false, data_type: 'VARCHAR(40)', column_default: nil },
        { column_name: 'numeric_col', is_nullable: false, data_type: 'NUMERIC(4,2)', column_default: nil },
        { column_name: 'arr', is_nullable: false, data_type: 'VARCHAR(255)[]', column_default: nil },
      ]
    end

    it "returns a Table instance with the same structure as the one on the DB" do
      table.create
      subject.should == table
    end

    it "should fetch the indexes too" do
      table.indexes = [
        PGx::Index.new(table, %w(row_id), primary: true),
        PGx::Index.new(table, %w(char_col), name: 'chars_index'),
        PGx::Index.new(table, %w(char_col varchar_col), unique: true),
      ]

      table.create
      table.create_indexes

      subject.indexes.should =~ table.indexes
    end
  end

  describe ".inject_raw_columns" do
    let(:columns) do
      [{ column_name: 'wei_column', pg_raw: { data_type: 'TEXT', expression: 'does not matter', is_nullable: true } },
       { column_name: 'mike_column', pg_raw: false },
       { column_name: 'jason_column', pg_raw: true }]
    end

    let(:expected_columns) do
      [{ column_name: 'wei_column', pg_raw: { data_type: 'TEXT', expression: 'does not matter', is_nullable: true } },
       { column_name: 'wei_column_raw', data_type: 'TEXT', is_nullable: true },
       { column_name: 'mike_column', pg_raw: false },
       { column_name: 'jason_column', pg_raw: true },
       { column_name: 'jason_column_raw' }]
    end

    specify { PGx::Table.inject_raw_columns(columns).should == expected_columns }
  end

  describe "#==" do
    it "is false if compared to something not a Table" do
      (subject == "").should be_false
    end

    it "is true when all the instance variables match" do
      (subject == subject.dup).should be_true
    end

    it "is false when any of the instance variables (except connection) does not match" do
      (subject == PGx::Table.new(base_name, temp: true)).should be_false
      (subject == PGx::Table.new(base_name, connection: 'foo')).should be_true
    end
  end

  shared_context "with columns and indexes" do
    let(:table_description) do
      {
        columns: columns,
        indexes: indexes,
        schema: TEST_SCHEMA_NAME,
        connection: connection,
        unlogged: false
      }
    end
    let(:table) { PGx::Table.new "diego_table", table_description }
    let(:columns) { [{ column_name: "wei_column" }, { column_name: "mike_column" }] }
    let(:indexes) { [{ columns: %w[wei_column] }] }
    let(:temp_table) { PGx::Table.new "diego_table", table_description.merge(temp: true) }
  end

  describe "#column_names" do
    include_context "with columns and indexes"
    subject { table.column_names }
    it { should == %w(wei_column mike_column) }
  end

  describe '#get_temp_table' do
    include_context "with columns and indexes"
    subject { table.get_temp_table }

    [:base_name, :schema, :columns, :connection].each do |attribute|
      it "returns a table with the same #{attribute}" do
        subject.send(attribute).should == table.send(attribute)
      end
    end

    it "also duplicates indexes and points them to the new table" do
      subject.indexes.each_index do |i|
        subject.indexes[i].should_not be(table.indexes[i])
        subject.indexes[i].table.should be(subject)
      end
    end
  end

  describe '#create' do
    include_context "with columns and indexes"

    it "should execute a create table query" do
      options = { column_array: table.columns, schema_name: table.schema }
      connection.should_receive(:exec).with(PGx::Connection.build_create_table_sql table.name, options)
      table.create
    end

    it "should build the SQL query with the given options" do
      PGx::Connection.should_receive(:build_create_table_sql) do |name, options|
        name.should == table.name
        options[:foo].should == 'bar'
        ';'
      end
      table.create foo: 'bar'
    end

    context "when columns contain pg_raw information" do
      let(:columns) do
        [{ column_name: 'wei_column', pg_raw: { data_type: 'TEXT', expression: 'does not matter', is_nullable: true } },
         { column_name: 'mike_column', pg_raw: true }]
      end

      it "executes a create table query" do
        column_array = [{ column_name: 'wei_column' },
                        { column_name: 'wei_column_raw', data_type: 'TEXT', is_nullable: true },
                        { column_name: 'mike_column' },
                        { column_name: 'mike_column_raw' }]

        options = { column_array: column_array, schema_name: table.schema }
        connection.should_receive(:exec).with(PGx::Connection.build_create_table_sql table.name, options)
        table.create
      end

      context "when pg_raw is set to false" do
        let(:columns) do
          [{ column_name: 'wei_column', pg_raw: false },
           { column_name: 'mike_column', pg_raw: { is_nullable: false } }]
        end

        it "skips raw columns if pg_raw is false" do
          column_array = [{ column_name: 'wei_column' },
                          { column_name: 'mike_column' },
                          { column_name: 'mike_column_raw', is_nullable: false }]

          options = { column_array: column_array, schema_name: table.schema }
          connection.should_receive(:exec).with(PGx::Connection.build_create_table_sql table.name, options)
          table.create
        end
      end
    end
  end

  describe '#drop' do
    include_context "with columns and indexes"

    it "should execute a drop table query" do
      options = { column_array: table.columns, schema_name: table.schema }
      connection.should_receive(:exec).with(PGx::Connection.build_drop_table_sql(table.name, options))
      table.drop
    end

    it "should build the SQL query with the given options" do
      PGx::Connection.should_receive(:build_drop_table_sql) do |name, options|
        name.should == table.name
        options[:foo].should == 'bar'
        ';'
      end
      table.drop foo: 'bar'
    end
  end

  describe '#exists?', with_schema: TEST_SCHEMA_NAME do
    include_context "with columns and indexes"

    it 'returns true if table exists' do
      table.create
      table.exists?.should be_true
    end

    it 'returns false if table does not exist' do
      table.exists?.should be_false
    end
  end

  describe '#insert' do
    include_context "with columns and indexes"

    let(:rows) { [] }

    context "when rows is an array" do
      let(:rows) { [column_array.zip(value_array)] }
      let(:column_array) { table.columns.map { |c| c[:column_name] } }
      let(:value_array) { %w[val1 val2] }

      it "executes a parametrized INSERT query" do
        sql = PGx::Connection.build_insert_into_pg_sql(table.name, column_array: column_array, schema_name: table.schema)
        connection.should_receive(:exec).with(sql, value_array)
        table.insert rows
      end
    end

    context "when rows is a hash" do
      let(:rows) { [{ wei_column: 'val1', mike_column: 'val2' }] }

      it "executes a parametrized INSERT query" do
        sql = PGx::Connection.build_insert_into_pg_sql(table.name, column_array: rows[0].keys, schema_name: table.schema)
        connection.should_receive(:exec).with(sql, rows[0].values)
        table.insert rows
      end
    end

    it "should do nothing if there are no values to insert" do
      connection.should_not_receive(:exec)
      table.insert rows
    end
  end

  describe "#insert_batch" do
    include_context "with columns and indexes"
    it 'runs in transactions' do
      rows = [[1], [2], [3], [4], [5]]
      connection.should_receive(:transaction).twice
      table.insert_batch([:column], rows, 3)
    end

    it "inserts in batches" do
      rows = [[1], [2], [3], [4], [5]]
      sql = PGx::Connection.build_insert_into_pg_sql(table.name, column_array: [:column], schema_name: table.schema)
      rows.each { |row| connection.should_receive(:exec).with(sql, row) }
      table.insert_batch([:column], rows, 3)
    end
  end

  describe '#insert_select' do
    include_context "with columns and indexes"

    let(:select_string) { '2 + 2 AS "result"' }

    it "executes a parametrized INSERT query" do
      connection.should_receive(:exec).with(
        PGx::Connection.build_insert_into_select_pg_sql(table.name, select_string, column_array: table.column_names, schema_name: table.schema),
        []
      )
      table.insert_select select_string
    end

    context "when column_names is given" do
      it "uses the column names" do
        connection.should_receive(:exec).with(
          PGx::Connection.build_insert_into_select_pg_sql(table.name, select_string, column_array: %w(mike_column), schema_name: table.schema),
          []
        )
        table.insert_select select_string, column_names: %w(mike_column)
      end
    end

    context "when options[:arguments] is given" do
      let(:args) { [1, 2, 'oops'] }
      it "passes on the args to exec" do
        connection.should_receive(:exec).with(
          PGx::Connection.build_insert_into_select_pg_sql(table.name, select_string, column_array: table.column_names, schema_name: table.schema),
          args
        )
        table.insert_select select_string, arguments: args
      end
    end

    context "when columns contain pg_raw information" do
      let(:value_array) { %w[val1 val1_raw val2 val2_raw] }

      let(:columns) do
        [{ column_name: 'wei_column', pg_raw: { data_type: 'TEXT', expression: 'does not matter', is_nullable: true } },
         { column_name: 'mike_column', pg_raw: true }]
      end

      it "executes an insert query" do
        column_names = %w(wei_column wei_column_raw mike_column mike_column_raw)

        connection.should_receive(:exec).with(
          PGx::Connection.build_insert_into_select_pg_sql(table.name, select_string, column_array: column_names, schema_name: table.schema),
          []
        )
        table.insert_select select_string
      end

      context "when pg_raw is set to false" do
        let(:value_array) { %w[val1 val1_raw val2] }

        let(:columns) do
          [{ column_name: 'wei_column', pg_raw: false },
           { column_name: 'mike_column', pg_raw: { is_nullable: false } }]
        end

        it "skips raw columns if pg_raw is false" do
          column_names = %w(wei_column mike_column mike_column_raw)

          connection.should_receive(:exec).with(
            PGx::Connection.build_insert_into_select_pg_sql(table.name, select_string, column_array: column_names, schema_name: table.schema),
            []
          )
          table.insert_select select_string
        end
      end
    end
  end

  describe '#update' do
    include_context "with columns and indexes"
    let(:rows) { [column_array.zip(value_array)] }
    let(:column_array) { table.columns.map { |c| c[:column_name] } }
    let(:value_array) { %w[val1 val2] }
    let(:expected_value_array) { %w[val3 val4] }

    it 'updates the rows' do
      table.with_connection do |t|
        t.create
        t.insert rows
      end

      table.with_connection { |t| t.update([column_array.zip(expected_value_array)]) }

      actual_results = PGx::Connection.connect do |connection|
        connection.exec "SELECT * FROM #{table.qualified_name};"
      end

      actual_results.values[0].should == expected_value_array
    end

    context 'when where_clause is specified' do

      it 'uses it for update' do
        table.with_connection do |t|
          t.create
          t.insert [column_array.zip(value_array), column_array.zip(%w[val0 val2])]
        end

        table.with_connection { |t| t.update([column_array.zip(expected_value_array)], where_clause: "wei_column='val0'") }

        actual_results = PGx::Connection.connect do |connection|
          connection.exec "SELECT * FROM #{table.qualified_name};"
        end

        actual_results.values.should =~ [value_array, expected_value_array]
      end

    end
  end

  describe '#create_primary_index' do
    include_context "with columns and indexes"

    let(:indexes) do
      [
        { columns: %w(wei_column mike_column) },
        { columns: %w(mike_column wei_column), primary: true },
        { columns: %w(wei_column), unique: true }
      ]
    end

    it "should create only the primary index" do
      table.indexes.each do |index|
        index.should_receive :create if index.primary?
        index.should_not_receive :create unless index.primary?
      end
      table.create_primary_index
    end
  end

  describe '#indexes=' do
    include_context "with columns and indexes"
    let(:indexes) { [] }

    it "parses index hashes into Index instances" do
      table.indexes = [{ columns: %w(wei_column) }]
      table.indexes[0].should == PGx::Index.new(table, %w(wei_column))
    end
  end

  describe '#copy_from', with_schema: TEST_SCHEMA_NAME do
    subject(:copied_table) { PGx::Table.allocate.copy_from original_table }

    let(:original_table) { FactoryGirl.build :table, :with_columns, :with_indexes, connection: connection }

    it "copies every instance variable except for the indexes" do
      copied_table.instance_variable_names.reject { |iv| iv == '@indexes' }.each do |name|
        expect(copied_table.instance_variable_get name).to eq original_table.instance_variable_get(name)
      end
    end

    describe "indexes" do
      it "have the same instance variables except for the table" do
        copied_table.indexes.size.should == original_table.indexes.size

        original_table.indexes.zip(copied_table.indexes).each do |original_index, copied_index|
          copied_index.equivalent_index?(original_index).should be_true
        end

      end

      it "point to the copied table" do
        copied_table.indexes.each { |index| index.table.should be_equal(copied_table) }
      end
    end
  end

  describe '#clone_rename', with_schema: TEST_SCHEMA_NAME do
    let(:new_table_name) { 'fancy_table_name' }
    subject(:cloned_table) { table.clone_rename new_table_name }

    let(:table) { FactoryGirl.build :table, :with_columns, :with_indexes, connection: connection }

    its(:name) { should == new_table_name }

    it "returns a copy of the original table" do
      cloned_table.instance_variable_names.reject { |iv| %w(@indexes @base_name).include?(iv) }.each do |name|
        cloned_table.instance_variable_get(name).should == table.instance_variable_get(name)
      end
    end

    it "regenerates the indexes' names" do
      cloned_table.indexes.each { |index| index.name.should include(new_table_name) }
    end

  end

  describe '#create_indexes', with_schema: TEST_SCHEMA_NAME do
    include_context "with columns and indexes"

    let(:indexes) { [{ columns: %w(wei_column mike_column) }, { columns: %w(mike_column wei_column) }] }

    before { table.create }

    it "should create each index on the table" do
      table.fetch_index_names.should be_empty
      table.create_indexes
      table.fetch_index_names.should =~ %w(idx_diego_table_on_wei_column_2 idx_diego_table_on_mike_column_1)
    end

    it "should skip an index if an equivalent index already exists" do
      equivalent_index = PGx::Index.new table, %w(wei_column mike_column), name: 'equivalent_index'
      equivalent_index.create

      table.indexes[0].should_not_receive(:create)
      table.indexes[1].should_receive(:create)

      table.create_indexes
    end
  end

  describe '#fetch_index_names', with_schema: TEST_SCHEMA_NAME do
    include_context "with columns and indexes"

    subject { table.fetch_index_names }

    let(:indexes) { [{ columns: %w[wei_column mike_column] }, { columns: %w[mike_column wei_column] }] }

    before do
      table.create
      table.create_indexes
    end

    it { should =~ %w(idx_diego_table_on_wei_column_2 idx_diego_table_on_mike_column_1) }

  end

  describe '#select' do
    include_context "with columns and indexes"

    it "should return the result of the query" do
      fake_result = double('result')
      connection.should_receive(:exec_and_log).and_return(fake_result)
      table.select.should == fake_result
    end

    it "should SELECT * when given no arguments" do
      connection.should_receive(:exec).with("SELECT * FROM #{table.qualified_name};", [])
      table.select
    end

    it "should use the first argument as the WHERE clause" do
      connection.should_receive(:exec).with("SELECT * FROM #{table.qualified_name} WHERE a = true;", [])
      table.select 'a = true'
    end

    it "should use the order argument as the ORDER BY clause" do
      connection.should_receive(:exec).with("SELECT * FROM #{table.qualified_name} ORDER BY a DESC, b ASC;", [])
      table.select order: 'a DESC, b ASC'
    end

    it "should use the group argument as the GROUP BY clause" do
      connection.should_receive(:exec).with("SELECT * FROM #{table.qualified_name} GROUP BY a;", [])
      table.select group: 'a'
    end

    it "should use the having argument as the HAVING clause" do
      connection.should_receive(:exec).with("SELECT * FROM #{table.qualified_name} HAVING count(1) > 1;", [])
      table.select having: 'count(1) > 1'
    end

    it "should use additional arguments as parameters to the query" do
      p1 = double('param1')
      p2 = double('param2')
      connection.should_receive(:exec).with("SELECT * FROM #{table.qualified_name} WHERE a = $1 AND b = $2;", [p1, p2])
      table.select 'a = $1 AND b = $2', p1, p2
    end

    describe 'options' do
      describe "columns:" do
        it "should select these columns instead of '*'" do
          connection.should_receive(:exec).with("SELECT a FROM #{table.qualified_name};", [])
          table.select columns: :a

          connection.should_receive(:exec).with("SELECT a, b FROM #{table.qualified_name};", [])
          table.select columns: %w(a b)
        end
      end
    end

    it "should handle having the where clause, arguments and options" do
      p1 = double('param1')
      p2 = double('param2')
      connection.should_receive(:exec).with(
        "SELECT c FROM #{table.qualified_name} WHERE a = $1 AND b = $2;", [p1, p2])
      table.select 'a = $1 AND b = $2', p1, p2, columns: 'c'
    end
  end

  describe '#select_simple' do
    include_context "with columns and indexes"

    it "should forward to call to #select, with the first argument as the :columns option" do
      table.should_receive(:select).with(:foo, bar: :baz, columns: 'a').and_return([])
      table.select_simple 'a', :foo, bar: :baz
    end

    it "should return requested column of the first row of the result" do
      expected_result = double('value')
      table.should_receive(:select).and_return([{ 'a' => expected_result }])
      table.select_simple(:a).should == expected_result
    end

    it "should return nil if the result is empty" do
      table.should_receive(:select).and_return([])
      table.select_simple('a').should == nil
    end
  end

  describe '#with_connection' do
    include_context "with columns and indexes"

    it "yields with self and the connection" do
      expect do |b|
        table.with_connection &b
      end.to yield_with_args(table, instance_of(PGx::Connection))
    end

    it "yields with an active connection which is cleared upon exit" do
      table.with_connection do |t|
        t.connection.should_not be_finished
      end
      expect { table.connection }.to raise_exception
    end

    it "returns the return value of the passed block" do
      result = double(:result)
      table.with_connection { |t| result }.should == result
    end

    context "when options presents" do
      it "invokes connect with options" do
        options = { a: 1, b: 2 }
        connection = double(:connection, close: nil)

        PGx::Connection.stub(:connect).and_return(connection)
        PGx::Connection.should_receive(:connect).with(options).and_yield(connection)

        expect do |b|
          table.with_connection(options, &b)
        end.to yield_control
      end
    end
  end

  describe '#hotswap', with_schema: TEST_SCHEMA_NAME do
    include_context "with columns and indexes"

    let(:temp_table) { table.get_temp_table }
    let(:records) { [{ wei_column: 1, mike_column: 'b' }, { wei_column: 2, mike_column: 'a' }] }

    before do
      temp_table.create
      temp_table.create_indexes
      temp_table.insert records

      table.drop check_exists: true
    end

    it 'drops the current table and replaces it with the temp table, alongside the indexes' do
      table.hotswap
      sql = "SELECT * FROM #{table.qualified_name};"
      connection.exec(sql).count.should == records.count
      table.fetch_index_names.should == %W(idx_#{table.name}_on_wei_column)
    end

    context "when columns have auto-increment sequences" do
      let(:columns) { [{ column_name: "wei_column", data_type: 'SERIAL' }, { column_name: "mike_column" }] }

      it "renames the sequence tables" do
        table.hotswap

        sql = "SELECT c.relname FROM pg_class c WHERE c.relkind = 'S';"
        sequences = PGx::Connection.connect { |c| c.exec(sql).entries.map { |e| e['relname'] } }
        sequences.should include("diego_table_wei_column_seq")
        sequences.should_not include("temp_diego_table_wei_column_seq")
      end
    end
  end

  describe '#insert_through_temp_table', with_schema: TEST_SCHEMA_NAME do
    include_context "with columns and indexes"

    let(:values_array) { [{ wei_column: 'foo', mike_column: 1 }, { wei_column: 'bar', mike_column: 2 }] }

    before { table.stub(:get_temp_table).and_return(temp_table) }

    it "should create, populate and index a temporary countries table, and hotswap it" do
      temp_table.should_receive(:drop).with(check_exists: true)
      temp_table.should_receive(:create)
      temp_table.should_receive(:insert).with(values_array)
      temp_table.should_receive(:create_indexes)
      temp_table.should_receive(:vacuum_analyze)
      table.should_receive(:hotswap)

      table.insert_through_temp_table values_array
    end
  end

  describe "#append_through_temp_table", with_schema: TEST_SCHEMA_NAME do
    include_context "with columns and indexes"

    let(:values_array) { [{ wei_column: 'foo', mike_column: 1 }, { wei_column: 'bar', mike_column: 2 }] }

    before { table.stub(:get_temp_table).and_return(temp_table) }

    context "when the live table already exists" do
      before { table.create }

      it "creates temp table and index, populates with data from live table, then hotswaps" do
        temp_table.should_receive(:drop).with(check_exists: true)
        temp_table.should_receive(:create)

        temp_table.should_receive(:insert_select).with("* FROM #{table.qualified_name}")

        temp_table.should_receive(:insert).with(values_array)
        temp_table.should_receive(:create_indexes)
        temp_table.should_receive(:vacuum_analyze)

        table.should_receive(:hotswap)

        table.append_through_temp_table values_array
      end
    end

    context "when the live table does not exist" do
      it "creates temp table and index, populates with data from live table, then hotswaps" do
        table.should_not_receive(:insert_select)
        table.append_through_temp_table values_array
      end
    end
  end

  describe "#to_hash" do
    include_context "with columns and indexes"

    it "should include everything needed to recreate the table" do
      table_hash = table.to_hash
      table_name = table_hash.delete(:table_name)
      PGx::Table.new(table_name, table_hash).should == table
    end
  end

  describe "#to_ruby_string" do
    include_context "with columns and indexes"
    subject { table.to_ruby_string(additional_keys) }

    let(:columns) do
      [
        { column_name: 'row_id', data_type: 'INT', column_default: 30, },
        { column_name: 'char_col', is_nullable: true, data_type: 'CHAR(20)', column_default: 'hello', },
        { column_name: 'varchar_col', is_nullable: false, data_type: 'VARCHAR(40)', column_default: nil, }
      ]
    end
    let(:indexes) { [] }
    let(:additional_keys) { [] }

    let(:expected_output) do
      <<-RUBY.strip_heredoc
      {
        columns: [
          { column_name: 'row_id',                          data_type: 'INT',         column_default: 30, },
          { column_name: 'char_col',    is_nullable: true,  data_type: 'CHAR(20)',    column_default: 'hello', },
          { column_name: 'varchar_col', is_nullable: false, data_type: 'VARCHAR(40)', },
        ],
        indexes: []
      }
      RUBY
    end
    it { should == expected_output }

    context "with indexes" do
      let(:indexes) do
        [
          { columns: %w[row_id], primary: true },
          { columns: %w[row_id char_col], name: 'foo_index' },
          { columns: %w[varchar_col], unique: true }
        ]
      end
      let(:expected_output) do
        <<-RUBY.strip_heredoc
        {
          columns: [
            { column_name: 'row_id',                          data_type: 'INT',         column_default: 30, },
            { column_name: 'char_col',    is_nullable: true,  data_type: 'CHAR(20)',    column_default: 'hello', },
            { column_name: 'varchar_col', is_nullable: false, data_type: 'VARCHAR(40)', },
          ],
          indexes: [
            { columns: ["row_id"],                                primary: true, },
            { columns: ["row_id", "char_col"], name: 'foo_index', },
            { columns: ["varchar_col"],                                          unique: true, },
          ]
        }
        RUBY
      end
      it { should == expected_output }
    end

    context "when expected keys are missing" do
      let(:columns) { [{ column_name: 'row_id', data_type: 'INT', column_default: 30 }] }
      let(:expected_output) do
        <<-RUBY.strip_heredoc
        {
          columns: [
            { column_name: 'row_id', data_type: 'INT', column_default: 30, },
          ],
          indexes: []
        }
        RUBY
      end
      it { should == expected_output }
    end

    context "when additional keys are provided" do
      let(:columns) { [{ column_name: 'row_id', data_type: 'INT', column_default: 30 }] }
      let(:additional_keys) { %w(h_expression pg_expression) }
      let(:expected_output) do
        <<-RUBY.strip_heredoc
        {
          columns: [
            { column_name: 'row_id', data_type: 'INT', column_default: 30, h_expression: '', pg_expression: '', },
          ],
          indexes: []
        }
        RUBY
      end
      it { should == expected_output }
    end

    context "when the columns is empty" do
      let(:columns) { [] }
      let(:expected_output) { '' }
      it { should == expected_output }
    end
  end

  describe "#to_sql" do
    include_context "with columns and indexes"
    subject { table.to_sql }

    let(:columns) do
      [
        { column_name: 'row_id', data_type: 'INT' },
        { column_name: 'char_col', is_nullable: true, data_type: 'CHAR(20)' },
        { column_name: 'varchar_col', is_nullable: false, data_type: 'VARCHAR(40)' }
      ]
    end
    let(:indexes) { [] }

    let(:expected_output) do
      <<-SQL.strip_heredoc
      CREATE TABLE #{table.qualified_name}
      ("row_id" INT NOT NULL,
       "char_col" CHAR(20),
       "varchar_col" VARCHAR(40) NOT NULL
       );
      SQL
    end
    it { should == expected_output }

    context "with indexes" do
      let(:indexes) do
        [
          { columns: %w[row_id], primary: true },
          { columns: %w[row_id char_col], name: 'foo_index' },
          { columns: %w[varchar_col], unique: true }
        ]
      end
      let(:expected_output) do
        <<-SQL.strip_heredoc
        CREATE TABLE #{table.qualified_name}
        ("row_id" INT NOT NULL,
         "char_col" CHAR(20),
         "varchar_col" VARCHAR(40) NOT NULL
         );
        CREATE UNIQUE INDEX "idx_#{table.name}_on_row_id" ON #{table.qualified_name} (row_id);
        ALTER TABLE #{table.qualified_name} ADD PRIMARY KEY USING INDEX "idx_#{table.name}_on_row_id";
        CREATE INDEX "foo_index" ON #{table.qualified_name} (row_id, char_col);
        CREATE UNIQUE INDEX "idx_#{table.name}_on_varchar_col" ON #{table.qualified_name} (varchar_col);
        SQL
      end
      it { should == expected_output }
    end
  end

  describe "#vacuum_analyze" do
    include_context "with columns and indexes"

    it "execute VACUUM ANALYZE on the table" do
      PGx::Connection.any_instance.should_receive(:exec_and_log).with("VACUUM ANALYZE #{table.qualified_name}")

      table.vacuum_analyze
    end

  end

end
