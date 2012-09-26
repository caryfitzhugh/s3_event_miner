#!/usr/bin/ruby

module Reducer

  def self.reduce_stream(input, output, error_output)
    item_count = 0
    stored_key = nil

    input.each_line do |line|
      (key, quantity) = line.split("\t")

      stored_key = key if stored_key.nil?

      if stored_key != key
        output.puts "#{stored_key}\t#{item_count}"
        item_count = quantity.to_i
        stored_key = key
      else
        item_count += quantity.to_i
      end
    end

    output.puts "#{stored_key}\t#{item_count}"
  end
end

if __FILE__ == $0
  Reducer.map_stream(ARGF, STDOUT, STDERR)
end
