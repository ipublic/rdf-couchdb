require 'rdf'
require 'enumerator'
require 'couchrest'

module RDF
  module CouchDB
    class Repository < ::RDF::Repository

      RDF_DESIGN_DOC_NAME = 'rdf_couchdb_repository'
      RDF_DESIGN_DOC_ID = "_design/#{RDF_DESIGN_DOC_NAME}"

      def self.map_function_for(parts)
        conditions = parts.sort.collect{|p| "(doc['#{p}'] || doc['#{p}']===null)"}.join(' && ')
        emit = 'emit(['+ parts.sort.collect{|p| "doc.#{p}"}.join(',') + '],1);'
        "function(doc) {
                  if (#{conditions}) {
                    #{emit}
                  }
                }"
      end

      RDF_DESIGN_DOC = {
        "_id" => RDF_DESIGN_DOC_ID,
        "language" => "javascript",
        "views" => {
          'all' => {
            'map' => map_function_for(%w(subject predicate object context))
          },
          'count' => {
            'map' => map_function_for(%w(subject predicate object context)),
            'reduce' => 'function(keys, values) {
                           return sum(values);
                        }'
          },
          'by_context' => {
            'map' => map_function_for(%w(context))
          },
          'by_object' => {
            'map' => map_function_for(%w(object))
          },
          'by_context_object' => {
            'map' => map_function_for(%w(object context))
          },
          'by_predicate' => {
            'map' => map_function_for(%w(predicate))
          },
          'by_context_predicate' => {
            'map' => map_function_for(%w(predicate context))
          },
          'by_object_predicate' => {
            'map' => map_function_for(%w(predicate object))
          },
          'by_context_object_predicate' => {
            'map' => map_function_for(%w(predicate object context))
          },
          'by_subject' => {
            'map' => map_function_for(%w(subject))
          },
          'by_context_subject' => {
            'map' => map_function_for(%w(subject context))
          },
          'by_object_subject' => {
            'map' => map_function_for(%w(subject object))
          },
          'by_context_object_subject' => {
            'map' => map_function_for(%w(subject object context))
          },
          'by_predicate_subject' => {
            'map' => map_function_for(%w(subject predicate))
          },
          'by_context_predicate_subject' => {
            'map' => map_function_for(%w(subject predicate context))
          },
          'by_object_predicate_subject' => {
            'map' => map_function_for(%w(subject predicate object))
          }
        }
      }

      def initialize(options = {})
        unless options[:database] && options[:database].instance_of?(CouchRest::Database)
          raise ArgumentError.new(":database option must be a CouchRest::Database required")
        end
        @database = options[:database]
        save_design_doc
      end

      def supports?(feature)
        case feature.to_sym
          when :context   then true   # statement contexts / named graphs
          when :inference then false  # forward-chaining inference
          else false
        end
      end      
      
      # @see RDF::Enumerable#each.
      def each(&block)
        if block_given?
          @database.view("#{RDF_DESIGN_DOC_NAME}/all", :include_docs=>true) do |result|
            subject = RDF::NTriples::Reader.unserialize(result['doc']['subject'])
            predicate = RDF::NTriples::Reader.unserialize(result['doc']['predicate'])
            object = RDF::NTriples::Reader.unserialize(result['doc']['object'])
            context = RDF::NTriples::Reader.unserialize(result['doc']['context'])
            block.call(RDF::Statement.new(subject, predicate, object, :context=>context))
          end          
        else
          ::Enumerable::Enumerator.new(self,:each)
        end
      end

      # @see RDF::Mutable#insert_statements
      # @param  [Array]
      # @return [void]      
      def insert_statements(statements)
        statements.each do |stmt|
          subject = RDF::NTriples::Writer.serialize(stmt.subject)
          predicate = RDF::NTriples::Writer.serialize(stmt.predicate)
          object = RDF::NTriples::Writer.serialize(stmt.object)
          context = RDF::NTriples::Writer.serialize(stmt.context)
          @database.save_doc({'subject'=>subject,
                               'predicate'=>predicate,
                               'object'=>object,
                               'context'=>context}, true)
        end
        @database.bulk_save
      end
      
      # @see RDF::Mutable#insert_statement
      def insert_statement(statement)
        insert_statements [statement]
      end

      # @see RDF::Mutable#delete_statement
      def delete_statement(statement)
        subject = RDF::NTriples::Writer.serialize(statement.subject)
        predicate = RDF::NTriples::Writer.serialize(statement.predicate)
        object = RDF::NTriples::Writer.serialize(statement.object)
        context = RDF::NTriples::Writer.serialize(statement.context)        
        @database.view("#{RDF_DESIGN_DOC_NAME}/all", :key=>[context, object, predicate, subject], :include_docs=>true) do |result|
          @database.delete_doc(result['doc'])          
        end
      end

      # @see RDF::Mutable#clear
      def clear_statements
        @database.view("#{RDF_DESIGN_DOC_NAME}/all", :include_docs=>true) do |result|
          result['doc']['_deleted'] = true
          @database.save_doc(result['doc'], true)
        end
        @database.bulk_save
      end

      ## Implementation of RDF::Queryable#query
      #  
      # This implementation will do well for statements and hashes, and not so
      # well for RDF::Query objects.
      # 
      # Accepts a query pattern argument as in RDF::Queryable.  See
      # {RDF::Queryable} for more information.
      #
      # @param [RDF::Statement, Hash, Array] pattern
      # @return [RDF::Enumerable, void]  
      # @see RDF::Queryable#query
      def query_pattern(pattern, &block)
        params = pattern.to_hash
        param_names = params.keys.select{|k| params[k]}.collect{|k| k.to_s}.sort
        if param_names.size==0
          view_name = "all"
          key = nil
        else
          view_name = "by_"+param_names.join('_')
          key = param_names.collect{|pn| RDF::NTriples::Writer.serialize(params[pn.to_sym])}
        end
        view_opts = { :include_docs => true }
        view_opts[:key] = key if key

        @database.view("#{RDF_DESIGN_DOC_NAME}/#{view_name}", view_opts) do |result|
          subject = RDF::NTriples::Reader.unserialize(result['doc']['subject'])
          predicate = RDF::NTriples::Reader.unserialize(result['doc']['predicate'])
          object = RDF::NTriples::Reader.unserialize(result['doc']['object'])
          context = RDF::NTriples::Reader.unserialize(result['doc']['context'])
          block.call(RDF::Statement.new(subject, predicate, object, :context=>context))
        end
      end

      ##
      # The number of statements in this repository
      # 
      # @see RDF::Enumerable#count
      # @return [Integer]
      def count
        view_result = @database.view("#{RDF_DESIGN_DOC_NAME}/count")
        if view_result['rows'] && view_result['rows'].first
          view_result['rows'].first['value']
        else
          0
        end
      end

    private

      def save_design_doc
        begin
          @database.get(RDF_DESIGN_DOC_ID)
        rescue => e
          design_doc = CouchRest::Design.new(RDF_DESIGN_DOC)
          design_doc.database = @database
          design_doc.save
        end
      end
    end
  end
end
