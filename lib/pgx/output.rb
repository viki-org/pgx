module PGx

  class Output < String
    attr_accessor :indentation

    def initialize
      @indentation = 0
    end

    def indentation= new_indentation
      @indentation = new_indentation < 0 ? 0 : new_indentation
    end

    def shift
      @indentation += shift_size
      self << (indentation_string * shift_size)
    end

    def unshift
      @indentation -= shift_size
      self.tap { |o| o.chomp!(indentation_string * shift_size) }
    end

    def newline
      self << "\n" << (indentation_string * indentation)
    end

    def append_hash_array hash_array, hash_keys, options = { }
      return self << '[]' if hash_array.empty?

      unless options[:additional_keys].nil? || options[:additional_keys].empty?
        base_hash = { }.tap { |h| options[:additional_keys].each { |k| h[k] = '' } }
        hash_array = hash_array.map { |col| base_hash.merge col }
        hash_keys = hash_keys + options[:additional_keys]
      end

      length_hash = Hash.new(0)
      hash_array.each do |hash|
        hash.each do |k, v|
          length = v.to_s.length
          length += 2 if v.is_a?(String)
          length_hash[k] = length if length_hash[k] < length
        end
      end

      self << "["
      self.newline.shift
      hash_array.each do |hash|
        self << "{ "
        (hash_keys & length_hash.keys).each do |k|
          v = hash[k]
          if v.nil?
            self << ' ' * (k.length + length_hash[k] + 4)
          else
            value = "#{v}"
            length = v.to_s.length
            if v.is_a?(String)
              quote_char = v.include?("'") ? '"' : "'"
              value = "#{quote_char}#{v}#{quote_char}"
              length += 2
            end
            spacing = length_hash[k] - length
            self << "#{k}: #{value},#{' ' * spacing} "
          end
        end
        self.rstrip!
        self << " },"
        self.newline
      end
      self.unshift
      self << "]"
    end

    private

    def shift_size
      2
    end

    def indentation_string
      ' '
    end

  end
end
