require 'rdf'
require 'enumerator'
require 'couchrest'

module RDF
  module CouchDB
    class Repository < ::RDF::Repository

      RDF_DESIGN_DOC_NAME = 'rdf_couchdb_repository'
      RDF_DESIGN_DOC_ID = "_design/#{RDF_DESIGN_DOC_NAME}"
      RDF_DESIGN_DOC = {
        "_id" => RDF_DESIGN_DOC_ID,
        "language" => "javascript",
        "views" => {
          'all' => {
            'map' => "function(doc) {
                  if ((doc['subject'] || doc['subject']===null) && (doc['predicate'] || doc['predicate']===null)
                      && (doc['object'] || doc['object']===null)  && (doc['context'] || doc['context']===null)) {
                    emit([doc.subject, doc.predicate, doc.object, doc.context],1);
                  }
                }"
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
          #TODO: produce an RDF::Statement, then:
          # block.call(RDF::Statement)
          #
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
        @database.view("#{RDF_DESIGN_DOC_NAME}/all", :key=>[subject, predicate, object, context], :include_docs=>true) do |result|
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
