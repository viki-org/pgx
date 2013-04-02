class Object
  def _? b = nil
    self
  end
end

class NilClass
  def _? b = nil
    return yield if block_given?
    b
  end
end

class Hash
  def self.symbolize_recursively! value
    if value.is_a? Hash
      value.symbolize_keys!
      value.values.each { |item| symbolize_recursively! item }
    elsif value.is_a? Array
      value.each { |item| symbolize_recursively! item }
    end
    value
  end

  def recursive_symbolize_keys!
    self.class.symbolize_recursively! self
  end
end
