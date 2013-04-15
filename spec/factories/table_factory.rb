require 'factory_girl'

FactoryGirl.define do
  factory :table, class: PGx::Table do
    ignore do
      base_name 'diego_table'
      columns []
      indexes []
      temp false
    end

    schema TEST_SCHEMA_NAME

    trait :with_columns do
      ignore { columns [
                         { column_name: 'wei_column',  is_nullable: false },
                         { column_name: 'mike_column', is_nullable: true }
                       ] }
    end

    trait :with_indexes do
      ignore { indexes [
                         { columns: %w(wei_column), primary: true },
                         { columns: %w(wei_column mike_column) },
                         { columns: %w(mike_column), name: 'jason_index', unique: true },
                         { columns: %w(mike_column), name: 'partial_index', where: 'mike_column IS NOT NULL'}
                       ] }
    end

    trait :temp do
      ignore { temp true }
    end

    initialize_with { new(base_name, columns: columns, indexes: indexes, temp: temp) }
  end
end
