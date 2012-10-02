# http://www.localytics.com/docs/querying-analytics-data-with-mapreduce/
# No _ in names
# Trailing / on input / output directories

S3_BUCKET = "s3-event-miner.ziplist.com"
ROOT_DIR  = "lib"
require 'aws-sdk'
#require 'aws/s3/base'
require './lib/s3_event_miner'
require 'elasticity'
require 'pry'

# Upload all these files to S3
AWS.config(
  :access_key_id     => ENV['AMAZON_ACCESS_KEY_ID'],
  :secret_access_key => ENV['AMAZON_SECRET_ACCESS_KEY']
)

def s3
  AWS::S3.new
end

def request_input(string, default)
  print "#{string} [default: #{default}]:  "
  input = STDIN.gets.strip
  if (input == "")
    default
  else
    input
  end
end

task :show do
  emr = Elasticity::EMR.new(ENV["AMAZON_ACCESS_KEY_ID"], ENV["AMAZON_SECRET_ACCESS_KEY"])
  flows = emr.describe_jobflows

  flows.each do |flow|
    next if ['COMPLETED', 'CANCELLED', 'FAILED', "TERMINATED"].include?(flow.state)
    puts
    puts "-"*80
    puts "#{flow.name}\n#{flow.last_state_change_reason}"
    puts "#{flow.jobflow_id}"
    flow.steps.each do |step|
      puts "  #{step.name}: #{step.state}"
    end
  end
end

task :kill do
  print "Id of job to terminate: "
  id = STDIN.gets.strip

  jobflow = Elasticity::JobFlow.from_jobflow_id(ENV["AMAZON_ACCESS_KEY_ID"], ENV["AMAZON_SECRET_ACCESS_KEY"], id)

  jobflow.shutdown
end

desc "create bucket"
task :create_bucket do
  bucket = nil
  begin
    bucket = s3.buckets[S3_BUCKET]
  rescue Exception => e
    puts e
    s3.buckets.create(S3_BUCKET, :acl => :authenticated_read)
    #AWS::S3::Bucket.create(S3_BUCKET, :access => :authenticated_read)
    bucket = s3.buckets[S3_BUCKET]
  end
end

desc "Cleanup everything.."
task :cleanup => :create_bucket do
  bucket = s3.buckets[S3_BUCKET]

  bucket.objects.each do |object|
    object.delete
    puts "There are #{bucket.objects.size} objects left in the bucket"
  end

  puts "Done deleting objects"

  bucket.delete
end

task :run => :create_bucket do


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

  token = Time.now.to_i
  outbucket = "jobs/#{selected_job[:name]}/#{token}/output/"
  scriptbucket = "jobs/#{selected_job[:name]}/#{token}/scripts/"
  inbucket = "jobs/#{selected_job[:name]}/#{token}/input/"

  puts "Input will be from:\n\n#{selected_job[:input]}\n\n"
  puts "Output will be in:\n\n#{outbucket}\n\n"

  confirm = request_input("About to upload all the files to the S3 bucket.  Enter 'yes' to continue. ", "yes")

  if (confirm == 'yes')
    # Sync Files
    puts "Uploading scripts..."
    bucket = s3.buckets[S3_BUCKET]

    puts "Searching input bucket for files..."
    Dir["#{File.dirname(__FILE__)}/jobs/#{selected_job[:name]}/**/*"].each do |local_file|
      bucket.objects["#{scriptbucket}#{File.basename(local_file)}"].write(File.read(local_file))
    end

    puts "Moving data files on S3 to input bucket"
    days = request_input("How many days back would you like to go on metrics:", 90).to_i

    inputsbucket, inputspath = selected_job[:input].split('/', 2)
    groups_of_pieces = (0..days).map do |day|
      start_day = Time.now - (60*60*24 * day)
      s3.buckets[inputsbucket].objects.with_prefix("#{inputspath}#{start_day.strftime("%Y-%m-%d")}")
    end.flatten

    groups_of_pieces.each do |pieces|
      pieces.each do |piece|
        puts "  #{File.basename(piece.key)} [#{piece.content_length / 1024.0}kb]"
        piece.copy_to("#{inbucket}#{File.basename(piece.key)}", :bucket_name => S3_BUCKET, :acl => :authenticated_read)
      end
    end

    puts "boom - done!"

    jobflow = Elasticity::JobFlow.new(ENV['AMAZON_ACCESS_KEY_ID'], ENV['AMAZON_SECRET_ACCESS_KEY'])
    jobflow.name = "#{selected_job[:name]} -- #{outbucket}"
    jobflow.placement                         = 'us-east-1a'

    loop do
      jobflow.instance_count = request_input("how many workers", 2).to_i
      jobflow.slave_instance_type = request_input("what kind of workers", 'm1.small')
      jobflow.master_instance_type = jobflow.slave_instance_type

      #jobflow.instance_count       = 2
      #jobflow.master_instance_type = 'm1.small'
      #jobflow.slave_instance_type  = 'm1.small'
      #jobflow.instance_count       = 8
      #jobflow.master_instance_type = 'c1.medium'
      #jobflow.slave_instance_type  = 'c1.medium'

      puts "Proposed Machine layout:"
      puts "Master type: #{jobflow.master_instance_type}"
      puts "Worker type: #{jobflow.slave_instance_type}"
      puts "Worker count: #{jobflow.instance_count}"

      confirm = request_input("Does this look good", 'yes')
      break if confirm == 'yes'
    end


    # Input bucket, output bucket, mapper and reducer scripts
    input_loc   = "s3://#{S3_BUCKET}/#{inbucket}"
    output_loc  = "s3://#{S3_BUCKET}/#{outbucket}"
    mapper      = "s3://#{S3_BUCKET}/#{scriptbucket}mapper.rb"
    reducer     = "s3://#{S3_BUCKET}/#{scriptbucket}reducer.rb"

    streaming_step = Elasticity::StreamingStep.new( input_loc, output_loc, mapper, reducer)
    jobflow.add_step(streaming_step)
    puts "Added streaming step.."


    puts "Job starting @ #{Time.now}!"

binding.pry

    jobflow.run

    still_running = true

    #sleep 5
    status = jobflow.status
    puts "#{status.name}\n#{status.jobflow_id}"
    puts "*"*100
    puts
    while still_running
      puts
      status = jobflow.status
      status.steps.each do |step|
        puts "  #{step.name}: #{step.state}"
      end
      puts "@ #{Time.now}"
      sleep 10

      still_running = !!status.steps.find {|s| s.state != "COMPLETED" && s.state != "FAILED" && s.state != "CANCELLED"}

      if (still_running)
        #print "\r\033[#{3 + status.steps.length}A"
      end

    end

    puts

    puts "Job completed @ #{Time.now}!"

    # The files all live in part-* in output
    parts = s3.buckets[S3_BUCKET].objects.with_prefix("#{outbucket}part-")

    # Show the total size of all the files
    total = parts.map {|p| p.content_length }.inject(0) {|sum, l|  sum + l }

    puts "Total size of results: #{total / 1024.0}kb"

    print "Output file to concat them all to locally: "
    output_file = STDIN.gets.strip

    # output_filename:
    File.open("output/" + output_file, "w") do |f|
      parts.each do |part|
        part.read do |chunk|
          f.write chunk
        end
      end
    end

    puts "Done! Enjoy"
  else
    puts "You needed to say 'yes'"
  end
end
