require 'spec_helper'

describe PGx::SQL do

  let(:claz) { Class.new{ extend PGx::SQL } }

  describe '.build_create_table_sql' do
    subject { claz.build_create_table_sql table_name, options }

    let(:table_name) { "table" }
    let(:options) { {} }

    it { should == %Q{CREATE TABLE "table"\n(\n );} }

    context "when table name is missing" do
      let(:table_name) { "" }
      it { should be_nil }
    end

    context "when given :column_array" do
      let(:options) {{
          column_array: [
              { column_name: "foo", is_nullable: true }
          ]
      }}

      it { should == %Q{CREATE TABLE "table"\n("foo" VARCHAR(255)\n );} }

      it "should handle several column_arrays" do
        options[:column_array] << { column_name: "bar", is_nullable: true }
        subject.should == %Q{CREATE TABLE "table"\n("foo" VARCHAR(255),\n "bar" VARCHAR(255)\n );}
      end
    end

    context "when given a :schema option" do
      let(:options) { {schema_name: "schema"} }
      it { should == %Q{CREATE TABLE "schema"."table"\n(\n );} }
    end

    context "when :like option is present" do
      let(:options) { {like: "schema.other_table"} }
      it { should == %Q{CREATE TABLE "table" (LIKE schema.other_table);} }
    end

    context "when unlogged" do
      let(:options) { { unlogged: true } }
      it { should == %Q{CREATE UNLOGGED TABLE "table"\n(\n );} }
    end
  end

  describe '.build_drop_table_sql' do
    subject { claz.build_drop_table_sql table, options }

    let(:table) { "table" }
    let(:options) { {} }

    it { should == %Q{DROP TABLE "table";} }

    context "when given a schema option" do
      let(:options) { {schema_name: "schema"} }
      it { should == %Q{DROP TABLE "schema"."table";} }
    end

    context "when :check_exists option is true" do
      let(:options) { {check_exists: true} }
      it { should == %Q{DROP TABLE IF EXISTS "table";} }
    end
  end

  describe '.build_alter_table_sql' do
    subject { claz.build_alter_table_sql table, options }

    let(:table) { "table" }
    let(:options) { {} }

    it { should be_nil }

    context "when renaming a table" do
      let(:options) { { rename_to: "other_table" } }
      it { should == 'ALTER TABLE "table" RENAME TO "other_table";' }

      context "when given a schema" do
        let(:options) { { rename_to: "other_table", schema_name: "schema" } }
        it { should == 'ALTER TABLE "schema"."table" RENAME TO "other_table";' }
      end
    end

    context "when adding a primary key on an index" do
      let(:options) { { primary_index: 'index' } }
      it { should == 'ALTER TABLE "table" ADD PRIMARY KEY USING INDEX "index";' }
    end

    context "when changing the schema" do
      let(:options) { { new_schema: 'new_schema' } }
      it { should == 'ALTER TABLE "table" SET SCHEMA "new_schema";' }
    end
  end

  describe '.build_create_index_sql' do
    subject { claz.build_create_index_sql index_name, table_name, options }

    let(:index_name) { "index" }
    let(:table_name) { "table" }
    let(:options) { {column_array: %w[column]} }

    it "should qualify the table name" do
      options[:schema_name] = "schema"
      should == %Q{CREATE INDEX "index" ON "schema"."table" (column);}
    end

    describe 'options' do

      describe ":column_array" do
        it "should handle a single column" do
          options[:column_array] = %w[column]
          should == %Q{CREATE INDEX "index" ON "table" (column);}
        end

        it "should handle multiple column_array" do
          options[:column_array] << "column2"
          should == %Q{CREATE INDEX "index" ON "table" (column, column2);}
        end
      end

      describe ":unique" do
        it "should enforce uniqueness constraint" do
          options[:unique] = true
          should match(/CREATE UNIQUE INDEX/)
        end
      end

    end
  end

  describe '.build_alter_index_sql' do
    subject { claz.build_alter_index_sql index_name, options }

    let(:index_name) { "index" }
    let(:options) { {} }

    describe 'options[:rename_to]' do
      before { options[:rename_to] = "other_index" }

      it { should == %Q{ALTER INDEX "index" RENAME TO "other_index";} }

      it "properly qualifies the index name" do
        options[:schema_name] = "schema"
        should == %Q{ALTER INDEX "schema"."index" RENAME TO "other_index";}
      end
    end
  end

  describe '.build_drop_index_sql' do
    subject { claz.build_drop_index_sql index, options }

    let(:index) { "index" }
    let(:options) { {} }

    it { should == %Q{DROP INDEX "index";} }

    context "when given a schema option" do
      let(:options) { {schema_name: "schema"} }
      it { should == %Q{DROP INDEX "schema"."index";} }
    end

    context "when :check_exists option is true" do
      let(:options) { {check_exists: true} }
      it { should == %Q{DROP INDEX IF EXISTS "index";} }
    end
  end

  describe '.build_insert_into_pg_sql' do
    subject { claz.build_insert_into_pg_sql table_name, options }

    let(:table_name) { "table" }
    let(:options) { {} }

    context "when table_name is missing" do
      let(:table_name) { "" }
      it { should be_nil }
    end

    context "when options are empty" do
      it { should be_nil }
    end

    it "qualifies the table name" do
      options[:schema_name] = "schema"
      options[:column_array] = %w[column]
      should == %Q{INSERT INTO "schema"."table"\n            ("column")\n     VALUES ($1);}
    end

    context "when given column_array" do
      it "should handle a single column" do
        options[:column_array] = %w[column]
        should == %Q{INSERT INTO "table"\n            ("column")\n     VALUES ($1);}
      end

      it "should handle multiple column_array" do
        options[:column_array] = %w[column1 column2]
        should == %Q{INSERT INTO "table"\n            ("column1", "column2")\n     VALUES ($1, $2);}
      end
    end
  end

  describe '.build_insert_into_select_pg_sql' do
    subject { claz.build_insert_into_select_pg_sql table_name, select_string, options }

    let(:table_name) { 'table' }
    let(:options) { {} }
    let(:select_string) { "TIMESTAMP '2012-05-25 03:59:59.999999' AS \"column\"" }

    context "when table_name is missing" do
      let(:table_name) { "" }
      it { should be_nil }
    end

    context "when options are empty" do
      it { should be_nil }
    end

    it "qualifies the table name" do
      options[:schema_name] = "schema"
      options[:column_array] = %w[column]
      should == %Q{INSERT INTO "schema"."table"\n            ("column")\nSELECT\n    TIMESTAMP '2012-05-25 03:59:59.999999' AS "column";}
    end
  end

  describe '.build_update_pg_sql' do
    subject { claz.build_update_pg_sql table_name, options }

    let(:table_name) { "diego_table" }
    let(:options) { {schema_name: 'schema'} }

    context "when table_name is missing" do
      let(:table_name) { "" }
      it { should be_nil }
    end

    context "when options are empty" do
      it { should be_nil }
    end

    context "when options[:column_array] is empty" do
      it { should be_nil }
    end

    it "qualifies the table name" do
      options[:column_array] = %w[column1 column2]
      should == <<-SQL.strip_heredoc.chomp
      UPDATE "schema"."diego_table"
        SET ("column1", "column2")
          = ($1, $2);
      SQL
    end

    context "when given where_clause" do
      it "should include the where_clause" do
        options[:where_clause] = "column1 = 5"
        options[:column_array] = %w[column1 column2]
        should == <<-SQL.strip_heredoc.chomp
        UPDATE "schema"."diego_table"
          SET ("column1", "column2")
            = ($1, $2)
          WHERE column1 = 5;
        SQL
      end
    end
  end

  describe '.build_column_sql' do
    subject { claz.build_column_sql @column_description }

    before do
      @column_description = {
        column_name: "foo",
        is_nullable: true,
      }
    end

    it "should have format column_name data_type [ column_constraint [ ... ] ]" do
      @column_description[:data_type] = "TYPE"
      @column_description[:constraints] = ["CONSTRAINT1", "CONSTRAINT2"]
      subject.should == '"foo" TYPE CONSTRAINT1 CONSTRAINT2'
    end

    it "should use the given type" do
      @column_description[:data_type] = "INT"
      subject.should == '"foo" INT'
    end

    it "should return nil if column_name missing" do
      @column_description[:column_name] = nil
      subject.should be_nil
    end

    it "sets the default if provided" do
      @column_description[:column_default] = "'mike''s bike'"
      subject.should match(/DEFAULT 'mike''s bike'/)
    end

    context "when the default is an integer" do
      it "sets the default" do
        @column_description[:data_type] = 'INT'
        @column_description[:column_default] = 0
        subject.should match(/DEFAULT 0/)
      end
    end

    context "when the default is a SQL function" do
      it "sets the default" do
        @column_description[:data_type] = 'TEXT'
        @column_description[:column_default] = 'NOW()'
        subject.should match(/DEFAULT NOW\(\)/)
      end
    end

    describe "column default" do
      before do
        @column_description = {
            column_name: "foo",
        }
      end

      it "is varchar of size 255" do
        subject.should match(/VARCHAR\(255\)/)
      end

      it "is not nullable" do
        subject.should match(/NOT NULL/)
      end
    end
  end

  describe '.get_qualified_relation_name' do
    subject { claz.get_qualified_relation_name relation_name, schema_name }

    let(:relation_name) { 'table' }
    let(:schema_name) { nil }

    it { should == '"table"' }

    context "when schema is provided" do
      let(:schema_name) { 'schema' }
      it { should == '"schema"."table"' }
    end
  end
end
