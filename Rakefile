require 'rubygems'
require 'rake/gempackagetask'

spec = Gem::Specification.new do |s|
  s.name = "kyotocabinet"
  s.version = "1.0.1"
  s.author = "Mikio Hirabayashi"
  s.homepage = "http://1978th.net/kyotocabinet/"
  s.email = "hirarin@gmail.com"
  s.extensions = [ "extconf.rb" ]
  s.files = [ "kyotocabinet.cc", "extconf.rb" ]
  s.has_rdoc = true
  s.require_paths = [ "." ]
  s.summary = "Kyoto Cabinet: a straightforward implementation of DBM."
end

task :default => [:package]

Rake::GemPackageTask.new(spec) do |pkg|
    pkg.need_tar = true
end
