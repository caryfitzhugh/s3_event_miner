S3_BUCKET = "s3-event-miner.ziplist.com"
ROOT_DIR  = "lib"

desc "Upload all the files to S3"
task :upload do
  require 'aws/s3'
  require 'aws/s3/base'

  # Upload all these files to S3
  AWS::S3::Base.establish_connection!(
    :access_key_id     => ENV['AMAZON_ACCESS_KEY_ID'],
    :secret_access_key => ENV['AMAZON_SECRET_ACCESS_KEY']
  )

  bucket = nil
  begin
    bucket = AWS::S3::Bucket.find(S3_BUCKET)
  rescue Exception => e
    puts e
    AWS::S3::Bucket.create(S3_BUCKET, :access => :authenticated_read)
    bucket = AWS::S3::Bucket.find(S3_BUCKET)
  end

  # Files locally
  Dir["#{ROOT_DIR}/**/*.rb"].each do |local_filename|
    local_file = File.open(local_filename)
    puts local_filename
    AWS::S3::S3Object.store(local_filename, open(local_filename), S3_BUCKET)
  end

  puts "Examine, before we delete this"
end
