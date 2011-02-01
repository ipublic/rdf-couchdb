$:.unshift File.dirname(__FILE__) + "/../lib/"

require 'rdf'
require 'rdf/spec/repository'
require 'rdf/couchdb'

describe RDF::CouchDB::Repository do
  context "A CouchDB Repository" do

    it 'should require a CouchRest database' do
      lambda { RDF::CouchDB::Repository.new }.should raise_error(ArgumentError)
    end
    
    context 'created with a CouchRest database' do
      before :each do
        @couchrest = CouchRest.new('http://localhost:5984')
        @database = @couchrest.database!('couchdb_rdf_test')
        @repository = RDF::CouchDB::Repository.new(:database=>@database)
      end

      it 'should create rdf respository design doc' do
        @database.get(RDF::CouchDB::Repository::RDF_DESIGN_DOC_ID).should_not be_nil        
      end

      it 'should update the rdf repository design doc is it has changed in code' do
        @database.delete_doc(@repository.design_doc)
        @database.save_doc({ '_id' => RDF::CouchDB::Repository::RDF_DESIGN_DOC_ID, 'views'=>{ }})
        @database.get(RDF::CouchDB::Repository::RDF_DESIGN_DOC_ID)['views'].should == { }
        @repository = RDF::CouchDB::Repository.new(:database=>@database)
        @database.get(RDF::CouchDB::Repository::RDF_DESIGN_DOC_ID)['views'].should_not be_nil
      end
      
      after :each do
        @database.recreate!
        # @repository.clear
      end

      # @see lib/rdf/spec/repository.rb in RDF-spec
      it_should_behave_like RDF_Repository
    end    

  end
end

