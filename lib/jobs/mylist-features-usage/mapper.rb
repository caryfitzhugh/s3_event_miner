#!/usr/bin/ruby

require 'uri'

module Mapper
  def self.map_stream(input, output, error_output)
    input.each_line do |line|
      begin
        timestamp,
        user_id,
        delim,
        ip,
        route,
        db_time,
        ttot,
        tvw,
        url    = line.split("\001")

        if (url =~ /mylist/)
          output.puts "total_mylist_actions"
        end

        # Only looking for things where the url is www
        uri = URI(url)

        result = catch(:search) do
          # Grouping Feature
          groupings = ['recipe', 'category', 'checklist', 'store', 'date_added', 'alphabetical', 'offer', 'user']
          groupings.each do |grouping|
            if (url =~ /mylist.*main_form.*grouping.*=#{grouping}/)
              throw(:search, "mylist_grouping_by_#{grouping}")
            end
          end

          # Form submit actions
          submit_actions = ['delete', 'edit', 'print', 'share_email', 'share_sms', 'update']
          submit_actions.each do |action|
            if (url =~ /mylist.*main_form.*name.*=submit_#{action}/)
              throw(:search, "mylist_form_action_#{action}")
            end
          end

          # Create list
          if (url =~ /lists\/new/)
            throw(:search, "mylist_create_list")
          end

          # Edit list
          if (url =~ /lists\/\d+\/edit/)
            throw(:search, "mylist_edit_list")
          end

          # Changing aisle order
          if (url =~ /mylist.*currently_viewed_store=\d+/)
            throw(:search, "mylist_changed_store_order")
          end

          # Filtering the list
          filters = ["checklist", "meal", "recipe", "store", "user"]
          filters.each do |filter|
            if (url =~ /mylist.*filters.*#{filter}/)
              throw(:search, "mylist_filtered_on_#{filter}")
            end
          end

          # Editing a list item
          if (url =~ /mylist.*update_listitems/)
            throw(:search, "mylist_edit_listitem")
          end

          if (url =~ /mylist.*more_filters/)
            throw(:search, "mylist_show_more_filters")
          end

          if (url =~ /mail_muncher/)
            throw(:search, "mylist_email_interface")
          end

          if (url =~ /zipbox_autocomplete/)
            throw(:search, "mylist_autocomplete")
          end

          if (url =~ /undo\//)
            throw(:search, "mylist_undo")
          end

          if (url =~ /undo_expansion/)
            throw(:search, "mylist_undo_expansion")
          end

          if (url =~ /mylist.*zipbox/)
            throw(:search, "mylist_zipbox_add")
          end
          nil
        end
        if (result)
          output.puts "#{result}"
        end
      rescue Exception => e
        error_output.puts "Unable to parse line: #{line}\n#{e}"
      end
    end
  end
end

Mapper.map_stream(ARGF, STDOUT, STDERR)
