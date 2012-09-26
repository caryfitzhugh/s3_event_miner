
module S3EventMiner
  @@jobs = []
  def self.configure_job(name, input)
    @@jobs << { :name => name, :input => input}
  end

  def self.jobs
    @@jobs
  end
end


Dir["#{File.dirname(__FILE__)}/jobs/**/config.rb"].each do |f|
  require f
end
