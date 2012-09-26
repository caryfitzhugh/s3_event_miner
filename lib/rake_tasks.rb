# http://www.localytics.com/docs/querying-analytics-data-with-mapreduce/

S3_BUCKET = "s3-event-miner.ziplist.com"
ROOT_DIR  = "lib"

desc "create bucket"
task :create_bucket do
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
  require 'lib/s3_event_miner'
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

  outbucket = "#{S3_BUCKET}/output/#{selected_job[:name]}/#{Time.now.to_i}"
  puts "Input will be from:\n\n#{selected_job[:input]}\n\n"
  puts "Output will be in:\n\n#{outbucket}\n\n"

  puts "About to upload all the files to the S3 bucket and run... ok? (yes)"

  confirm = STDIN.gets.strip

  if (confirm == 'yes')
    # Sync Files
    s3 = Elasticity::SyncToS3.new(S3_BUCKET,ENV['AMAZON_ACCESS_KEY_ID'], ENV['AMAZON_SECRET_ACCESS_KEY'])
    puts "Uploading job..."
    s3.sync("#{File.dirname(__FILE__)}/jobs/#{selected_job[:name]}", "data/jobs/#{selected_job[:name]}")
    puts "boom - done!"

    jobflow = Elasticity::JobFlow.new(ENV['AMAZON_ACCESS_KEY_ID'], ENV['AMAZON_SECRET_ACCESS_KEY'])
    jobflow.name = "#{selected_job[:name]} -- #{outbucket}"
    jobflow.placement                         = 'us-east-1a'
    jobflow.instance_count       = 2
    jobflow.master_instance_type = 'm1.small'
    jobflow.slave_instance_type  = 'm1.small'

    # Input bucket, output bucket, mapper and reducer scripts
    input_loc = "s3n://#{selected_job[:input]}"
    output_loc= "s3n://#{outbucket}"
    mapper    = "s3n://#{S3_BUCKET}/data/jobs/#{selected_job[:name]}/mapper.rb"
    reducer   = "s3n://#{S3_BUCKET}/data/jobs/#{selected_job[:name]}/reducer.rb"
    puts "input:"
    puts input_loc
    puts "\noutput:"
    puts output_loc
    puts "\nmapper:"
    puts mapper
    puts "\nreducer:"
    puts reducer

    #   streaming_step = Elasticity::StreamingStep.new( input_loc, output_loc, mapper, reducer)
    #   jobflow.add_step(streaming_step)
    #   puts "Added streaming step.."
    #   puts streaming_step
    #
    #   puts "Starting..."
    #   jobflow.run
    #   puts "Running..."

  else
    puts "You needed to say 'yes'"
  end
end
