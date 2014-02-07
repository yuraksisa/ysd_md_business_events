require 'data_mapper' unless defined?DataMapper

module BusinessEvents

 #
  # Represents the status of a BusinessEvent for an specific command
  #
  class BusinessEventProcess
    include DataMapper::Resource
    
    storage_names[:default] = 'be_business_event_process'
    
    property :id, Serial

    #belongs_to :business_event, 'BusinessEvents::BusinessEvent', :child_key => [:business_event_id],
    #  :parent_key => [:id]
    
    property :command_name, String, :field => 'command_name', :length => 32
    property :autoexecute, Boolean, :field => 'autoexecute', :default => false
    property :status, String, :field => 'status', :length => 10, :default => :PENDING
    property :last_update, DateTime, :field => 'last_update'
    property :error, String, :field => 'error', :length => 255       
 
    
    #
    # Before update 
    #
    before :save do
      self.last_update = Time.now
    end    
    
    #
    # Process the command
    #
    def execute 
       
      puts "Executing the command : #{business_event.event} #{self.command_name} #{business_event.date} #{business_event.data}" 
      
      if command = BusinessEvent.get_command(business_event.event, self.command_name)
      
         command_class = command[:command_class]
         command_instance = command_class.new(business_event)     
       
         begin
           puts "Executing the command #{command_instance.class.name}"
           command_instance.execute
           attribute_set(:status, :DONE)
           puts "Command executed successfully"
         rescue
           puts "Error executing the command"
           attribute_set(:status, :ERROR)
           attribute_set(:error, $!)
         end
       
         # Update the data
         if self.saved?
           self.save
         end
         
      end
        
    end
    
  end

end