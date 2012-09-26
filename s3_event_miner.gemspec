# -*- encoding: utf-8 -*-
require File.expand_path('../lib/s3_event_miner/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Cary FitzHugh"]
  gem.email         = ["cary.fitzhugh@ziplist.com"]
  gem.description   = %q{Mine S3 Events}
  gem.summary       = %q{Mine S3 Events}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "s3_event_miner"
  gem.require_paths = ["lib"]
  gem.version       = S3EventMiner::VERSION

  gem.add_development_dependency     "pry"
  gem.add_runtime_dependency     "aws-s3"
end
