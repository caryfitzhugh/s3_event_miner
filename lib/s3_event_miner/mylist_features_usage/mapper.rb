#!/usr/bin/ruby

module Mapper
  def self.map_stream(input, output, error_output)
    input.each_line do |line|
      begin
        blob = JSON.parse(line)
        output.puts "#{blob["user_id"]}\t#{blob["quantity"]}"
      rescue
        error_output.puts "Unable to parse line: #{line}"
      end
    end
  end
end

if __FILE__ == $0
  Mapper.map_stream(ARGF, STDOUT, STDERR)
end
