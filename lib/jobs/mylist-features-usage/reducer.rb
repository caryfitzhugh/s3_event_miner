#!/usr/bin/ruby

STDERR.puts "Starting Reducer"

module Reducer

  def self.reduce_stream(input, output, error_output)
    item_count = 0
    stored_key = nil

    input.each do |key|
      key = key.strip
      stored_key = key if stored_key.nil?

      if stored_key != key
        output.puts "#{stored_key}\t#{item_count}"
        item_count = 1
        stored_key = key
      else
        item_count += 1
      end
    end

    output.puts "#{stored_key}\t#{item_count}"
  end
end

Reducer.reduce_stream(ARGF, STDOUT, STDERR)
