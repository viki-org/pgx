require 'spec_helper'

describe PGx::Output do
  subject { PGx::Output.new }

  describe "#indentation" do
    it "can be set" do
      expect{ subject.indentation = 3 }.to change{ subject.indentation }.from(0).to(3)
    end

    it "can't be less than zero" do
      subject.indentation = 2
      expect{ subject.indentation = -3 }.to change{ subject.indentation }.from(2).to(0)
    end
  end

  describe '#shift' do
    it "appends indentation" do
      expect{ subject.shift }.to change{ subject.dup }.from("").to("  ")
    end

    it "increases the indentation level by 2" do
      expect{ subject.shift }.to change{ subject.indentation }.by(2)
    end
  end

  describe '#unshift' do
    it "chomps indentation" do
      subject << "    "
      expect{ subject.unshift }.to change{ subject.dup }.from("    ").to("  ")
    end

    it "decreases the indentation level by 2" do
      subject.indentation = 1
      expect{ subject.unshift }.to change{ subject.indentation }.by(-2)
    end
  end

  describe '#newline' do
    it "appends a newline to the output" do
      expect{ subject.newline }.to change{ subject.dup }.from("").to("\n")
    end

    context "when there is indentation" do
      before { subject.indentation = 2 }

      it "should indent after the newline" do
        expect{ subject.newline }.to change{ subject.dup }.from("").to("\n  ")
      end
    end
  end

  describe '#append_hash_array' do
    subject { output.append_hash_array hash_array, hash_keys }
    let(:output) { PGx::Output.new }
    let(:hash_array) {
      [
        {foo: 1, bar: 2},
        {foo: 3, baz: 4},
        {foo: 5, bar: 6, baz: 7}
      ]
    }
    let(:hash_keys) { [:foo, :bar, :baz] }

    let(:expected_output) do
      string = <<-RUBY.strip_heredoc
      [
        { foo: 1, bar: 2, },
        { foo: 3,         baz: 4, },
        { foo: 5, bar: 6, baz: 7, },
      ]
      RUBY
      string.chomp "\n"
    end
    it { should == expected_output }

    context "when array is empty" do
      let(:hash_array) { [] }
      it { should == "[]" }
    end

    context "when expected keys are missing" do
      let(:hash_keys) { [:foo, :not_in_hashes, :bar, :baz] }
      it { should == expected_output }
    end

    describe "options" do
      subject { output.append_hash_array hash_array, hash_keys, options }

      describe ":additional_keys" do
        let(:options) { {additional_keys: [:cat]} }
        let(:expected_output) do
          string = <<-RUBY.strip_heredoc
            [
              { foo: 1, bar: 2,         cat: '', },
              { foo: 3,         baz: 4, cat: '', },
              { foo: 5, bar: 6, baz: 7, cat: '', },
            ]
          RUBY
          string.chomp "\n"
        end
        it { should == expected_output }
      end
    end
  end

end
