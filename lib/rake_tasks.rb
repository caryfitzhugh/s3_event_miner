# http://www.localytics.com/docs/querying-analytics-data-with-mapreduce/
# No _ in names
# Trailing / on input / output directories

S3_BUCKET = "s3-event-miner.ziplist.com"
ROOT_DIR  = "lib"
require 'aws/s3'
require 'aws/s3/base'

# Upload all these files to S3
AWS::S3::Base.establish_connection!(
  :access_key_id     => ENV['AMAZON_ACCESS_KEY_ID'],
  :secret_access_key => ENV['AMAZON_SECRET_ACCESS_KEY']
)

desc "create bucket"
task :create_bucket do

  bucket = nil
  begin
    bucket = AWS::S3::Bucket.find(S3_BUCKET)
  rescue Exception => e
    puts e
    AWS::S3::Bucket.create(S3_BUCKET, :access => :authenticated_read)
    bucket = AWS::S3::Bucket.find(S3_BUCKET)
  end
end

desc "Cleanup everything.."
task :cleanup => :create_bucket do
  bucket = AWS::S3::Bucket.find(S3_BUCKET)

  bucket.objects.each do |object|
    object.delete
    puts "There are #{bucket.objects.size} objects left in the bucket"
  end

  puts "Done deleting objects"

  bucket.delete
end

task :run => :create_bucket do
  require './lib/s3_event_miner'
  require 'elasticity'


  puts "Which do you want to run?"
  i = 0
  jobs = S3EventMiner.jobs

  jobs.each do |job|
    puts "#{i}) #{job[:name]}"
    i+=1
  end

  print "Select a job: "
  i = STDIN.gets.to_i

  selected_job = jobs[i]
  puts "Selected: #{selected_job[:name]}"

  outbucket = "output/#{selected_job[:name]}/#{Time.now.to_i}/"
  puts "Input will be from:\n\n#{selected_job[:input]}\n\n"
  puts "Output will be in:\n\n#{outbucket}\n\n"

  puts "About to upload all the files to the S3 bucket and run... ok? (yes)"

  confirm = STDIN.gets.strip

  if (confirm == 'yes')
    # Sync Files
    s3 = Elasticity::SyncToS3.new(S3_BUCKET,ENV['AMAZON_ACCESS_KEY_ID'], ENV['AMAZON_SECRET_ACCESS_KEY'])
    puts "Uploading job..."
    s3.sync("#{File.dirname(__FILE__)}/jobs", "data/jobs")

    puts "boom - done!"

    jobflow = Elasticity::JobFlow.new(ENV['AMAZON_ACCESS_KEY_ID'], ENV['AMAZON_SECRET_ACCESS_KEY'])
    jobflow.name = "#{selected_job[:name]} -- #{outbucket}"
    jobflow.placement                         = 'us-east-1a'
    jobflow.instance_count       = 2
    jobflow.master_instance_type = 'm1.small'
    jobflow.slave_instance_type  = 'm1.small'

    # Input bucket, output bucket, mapper and reducer scripts
    input_loc = "s3://#{selected_job[:input]}"
    output_loc= "s3://#{S3_BUCKET}/#{outbucket}"
    mapper    = "s3://#{S3_BUCKET}/data/jobs/#{selected_job[:name]}/mapper.rb"
    reducer   = "s3://#{S3_BUCKET}/data/jobs/#{selected_job[:name]}/reducer.rb"
    bootstrap    = "s3://#{S3_BUCKET}/data/jobs/bootstrap_ruby.sh"

    streaming_step = Elasticity::StreamingStep.new( input_loc, output_loc, mapper, reducer)
    jobflow.add_step(streaming_step)
    puts "Added streaming step.."


    puts "Job starting @ #{Time.now}!"

    jobflow.run

    still_running = true

    while still_running
      sleep 10
      puts "*"*100
      status = jobflow.status
      puts "#{status.name}\n#{status.jobflow_id}\n#{Time.now}"
      jobflow.status.steps.each do |step|
        puts "  #{step.name}: #{step.state}"
      end

      still_running = !!status.steps.find {|s| s.state != "COMPLETED" && s.state != "FAILED" }
    end

    puts "Job completed @ #{Time.now}!"

    require 'pry'
    require 'pry-nav'
    binding.pry
    # The files all live in part-* in output
    parts = AWS::S3::Bucket.objects(S3_BUCKET, :prefix => "#{outbucket}part-")

    # Show the total size of all the files
    total = parts.map {|p| p.about['content-length'].to_i }.inject(0) {|sum, l|  sum + l }

    puts "Total size of results: #{total / 1024.0}kb"
    binding.pry

    print "Output file to concat them all to locally: "
    output_file = STDIN.gets.strip

    # output_filename:
    File.open("output/" + output_file, "w") do |f|
      parts.each do |part|
        part.value do |chunk|
          f.write chunk
        end
      end
    end

    puts "Done! Enjoy"
    binding.pry
  else
    puts "You needed to say 'yes'"
  end
end
