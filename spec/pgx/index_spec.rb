require 'spec_helper'

describe PGx::Index do
  subject{ index }

  let(:index) { PGx::Index.new table, column_names, options }
  let(:table) { FactoryGirl.build :table, :with_columns, connection: connection }
  let(:column_names) { %w(wei_column mike_column) }
  let(:options) { {} }

  let(:connection) { PGx::Connection.connect }
  after { connection.close }

  it { should be }
  its(:table) { should == table }
  its(:column_names) { should == %w(wei_column mike_column) }
  its(:schema) { should == table.schema }
  its(:qualified_name) { should == %Q{"#{table.schema}"."#{index.name}"} }
  it { should_not be_primary }
  it { should_not be_unique }

  context "when not given a table" do
    let(:index) { PGx::Index.new 'foo', column_names, options }
    it { expect{ subject }.to raise_error }
  end

  describe "options" do

    describe ':schema' do
      let(:options) { { schema: schema } }
      let(:schema) { TEST_SCHEMA_NAME }

      its(:schema) { should == schema }
      its(:qualified_name) { should == %Q{"#{schema}"."#{index.name}"} }
    end

    describe ':primary' do
      let(:options) { { primary: true } }

      it { should be_unique }
      it { should be_primary }
    end

    describe ':unique' do
      let(:options) { { unique: true } }

      it { should be_unique }
    end
  end

  describe '.fetch', with_schema: TEST_SCHEMA_NAME do
    let(:table) { FactoryGirl.build :table, :with_columns, :with_indexes, connection: connection }

    it "returns an Index instance with the same structure as the one on the DB" do
      table.create
      table.create_indexes

      table.indexes.each do |index|
        PGx::Index.fetch(connection, table, index.name).should == index
      end
    end

    context "when the index definitions are more than columns" do
      let(:index_name) { "complicated_index_on_#{table.name}" }
      before do
        table.create
        PGx::Connection.connect do |connection|
          connection.exec "CREATE INDEX #{index_name} ON #{table.qualified_name} (wei_column, lower(mike_column))"
        end
      end

      it "extracts the column info correctly" do
        PGx::Index.fetch(connection, table, index_name).column_names.should == %w(wei_column lower(mike_column::text))
      end
    end
  end

  describe "#==" do
    it "is false if compared to something not an Index" do
      (subject == "").should be_false
    end

    it "is true when all the instance variables match" do
      (subject == subject.dup).should be_true
    end

    it "is false when any of the instance variables does not match" do
      other_index = index.dup
      other_index.name = 'foo'
      (subject == other_index).should be_false
    end
  end

  describe "#name" do
    subject { index.name }
    let(:column_names) { %w(wei_column) }

    context "when given a single column" do
      it { should == 'idx_diego_table_on_wei_column' }
    end

    context "when given multiple columns" do
      let(:column_names) { %w(wei_column mike_column) }
      it { should == 'idx_diego_table_on_wei_column_2' }
    end

    context "when the index name is specified" do
      let(:options) { { name: 'json_index' } }
      it { should == 'json_index' }
    end

    context "when the table is a temp table" do
      let(:table) { FactoryGirl.build :table, :with_columns, :temp }

      it { should == 'temp_idx_diego_table_on_wei_column' }

      context "when the index name is specified" do
        let(:options) { { name: 'json_index' } }

        it{ should == 'temp_json_index' }
      end
    end
  end

  describe '#exists?', with_schema: TEST_SCHEMA_NAME do
    before { table.create }

    it 'returns true if index exists' do
      index.create
      index.exists?.should be_true
    end

    it 'returns false if index does not exist' do
      index.exists?.should be_false
    end
  end

  describe '#equivalent_index?' do
    it "is true when the table, column names, primary and unique flags match" do
      subject.dup.tap do |dup|
        dup.name = 'json_index'
        subject.equivalent_index?(dup).should be_true
      end
    end

    it "is false when any of the table name, schema, column names, primary and unique flags differ" do
      [:column_names=, :primary=, :unique=].each do |attr|
        dup = subject.dup
        dup.send attr, 'foo'
        subject.equivalent_index?(dup).should be_false
      end

      dup = subject.dup
      dup.table = FactoryGirl.build :table, base_name: 'foo'
      subject.equivalent_index?(dup).should be_false

      dup = subject.dup
      dup.table = FactoryGirl.build :table, schema: 'foo'
      subject.equivalent_index?(dup).should be_false
    end
  end

  describe '#create' do
    before do
      PGx::Connection.any_instance.stub(:exec)
      PGx::Connection.any_instance.stub(:index_exists?).and_return(false)
    end

    subject { index.create }
    let(:columns_string) { columns_string = index.column_names.join(', ') }

    it "should execute a CREATE INDEX statement" do
      connection.should_receive(:exec).with(%Q{CREATE INDEX "idx_diego_table_on_wei_column_2" ON #{table.qualified_name} (#{columns_string});})
      subject
    end

    context "when :primary is true" do
      let(:options) { { primary: true } }
      it "should create a UNIQUE index and alter the table" do
        connection.should_receive(:exec).with(%Q{CREATE UNIQUE INDEX "idx_diego_table_on_wei_column_2" ON #{table.qualified_name} (#{columns_string});})
        connection.should_receive(:exec).with(%Q{ALTER TABLE #{table.qualified_name} ADD PRIMARY KEY USING INDEX "idx_diego_table_on_wei_column_2";})
        subject
      end
    end

    context "when :unique is true" do
      let(:options) { { unique: true } }
      it "should create a UNIQUE index and not alter the table" do
        connection.should_receive(:exec).with(%Q{CREATE UNIQUE INDEX "idx_diego_table_on_wei_column_2" ON #{table.qualified_name} (#{columns_string});})
        connection.should_not_receive(:exec).with(%Q{ALTER TABLE #{table.qualified_name} ADD PRIMARY KEY USING INDEX "idx_diego_table_on_wei_column_2";})
        subject
      end
    end

    context "when the index already exists" do
      it "drops index first" do
        PGx::Connection.any_instance.stub(:index_exists?).and_return(true)
        index.should_receive(:drop)
        subject
      end
    end

  end

  describe '#drop' do
    it "should execute a DROP INDEX query" do
      connection.should_receive(:exec).with("DROP INDEX #{index.qualified_name};")
      index.drop
    end

    describe 'options' do
      describe ':check_exists' do
        it "should add a IF EXISTS parameter to the statement" do
          connection.should_receive(:exec).with("DROP INDEX IF EXISTS #{index.qualified_name};")
          index.drop check_exists: true
        end
      end
    end
  end

  describe '#rename' do
    it "should execute an ALTER INDEX statement" do
      connection.should_receive(:exec).with(%Q{ALTER INDEX #{index.qualified_name} RENAME TO "jason_index";})
      index.rename 'jason_index'
    end
  end

  describe "#to_hash" do
    subject { index.to_hash }

    it { should == { table_name: index.table.name, schema: index.schema, columns: index.column_names } }

    context "when index has an overridden name" do
      let(:options) { {name: 'jason_index'} }
      it { should == { table_name: index.table.name, schema: index.schema, columns: index.column_names, name: 'jason_index' } }
    end

    context "when index has a overridden name that matches the generated name" do
      let(:options) { {name: 'idx_diego_table_on_wei_column_2'} }
      it { should == { table_name: index.table.name, schema: index.schema, columns: index.column_names } }
    end

    context "when primary" do
      let(:options) { {primary: true} }
      it { should == { table_name: index.table.name, schema: index.schema, columns: index.column_names, primary: true } }
    end

    context "when unique" do
      let(:options) { {unique: true} }
      it { should == { table_name: index.table.name, schema: index.schema, columns: index.column_names, unique: true } }

      context "when primary" do
        let(:options) { {unique: true, primary: true} }
        it { should == { table_name: index.table.name, schema: index.schema, columns: index.column_names, primary: true } }
      end
    end
  end

end
