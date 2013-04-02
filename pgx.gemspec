# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

require 'pgx'

Gem::Specification.new do |s|
  s.name          = "pgx"
  s.version       = "0.0.1" #PGx::VERSION
  s.authors       = ["Huy Nguyen"]
  s.email         = %w(huy@viki.com)
  s.description   = %q{TODO: Write a gem description}
  s.summary       = %q{TODO: Write a gem summary}
  s.homepage      = ""

  s.files         = `git ls-files`.split($/)
  s.executables   = s.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  s.test_files    = s.files.grep(%r{^(test|spec|features)/})
  s.require_paths = %w(lib)

  s.add_development_dependency "rspec"
  s.add_development_dependency "factory_girl"

  s.add_runtime_dependency "log4r"
  s.add_runtime_dependency "pg"
  s.add_runtime_dependency "activesupport"

end
