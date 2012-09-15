require 'rubygems'
require 'data_mapper'
require 'dm-constraints'
module BusinessEvents

  #
  # It represents a system event
  #
  # The commands are registering using the register_command method
  #
  class BusinessEvent
    include DataMapper::Resource
    
    @commands = {} # Define the registered commands hash
    
    storage_names[:default] = 'be_business_event'
   
    property :id, Serial, :field => 'id', :key => true
    property :event, String, :field => 'event', :length => 32
    property :data, String, :field => 'data', :length => 1024 # It's the json representation of the data
    property :date, DateTime, :field => 'date'              # When the event has happened
    has n, :business_event_processes, 'BusinessEventProcess', :child_key => [:business_event_id] , :parent_key => [:id], :constraint => :destroy
   
    # ================= Finders =================================
    
    #
    # @param [Hash] options
    #   
    #   :limit
    #   :offset
    #   :count
    #
    # @return [Array]
    #    
    def self.find_all(options={})
        
      limit = options[:limit] || 10
      offset = options[:offset] || 0
      count = options[:count] || true     
   
      result = []
      
      result << BusinessEvent.all({:limit => limit, :offset => offset, :order => [:date.desc]})
      
      if count
        result << BusinessEvent.count
      end
      
      if result.length == 1
        result = result.first
      end
      
      result
   
   
    end
   
    # ----------------- Instance methods ------------------------
   
    #
    # Process a command 
    #
    def process
     
      business_event_processes.each do |process| 
        unless process.status == :DONE 
          process.execute
        end
      end
          
    end
   
    # ------------------- Class methods -------------------------
   
    #
    # Notify a Business Event
    #
    # @param [Symbol] event
    #   The event notified
    #
    # @param [Object] data
    #   The data associated to the event
    #
    def self.fire_event(event, data)
   
      unless event.is_a?Symbol
        event = event.to_sym
      end
   
      # Saves the business event with all the processors           
      businessEvent = BusinessEvent.new(:event => event, :data => data.to_json, :date => Time.now)
   
      if the_commands = commands[event]
        the_commands.each do |key, value|
           options = value[:options]
           be_process = BusinessEventProcess.new(:command_name => key, :autoexecute => options[:autoexecute] || false)
           businessEvent.business_event_processes << be_process        
        end
      else
        puts "No commands for #{event}"
      end  
      
      BusinessEvent.transaction do |transaction|
        businessEvent.save        
        transaction.commit
      end

      # Executes the autoexecuted processes (Maybe it should be done in other thread/process)
      businessEvent.business_event_processes.each do |be_process|   
        be_process.execute if be_process.autoexecute
      end
   
      businessEvent
      
    end
   
    #
    # Register a command that will process a event
    #
    # @param [Symbol] event
    #   The event that the command will process
    #
    # @param [Symbol] name
    #   The command name
    #
    # @param [BusinessEventCommand]
    #
    # @param [Hash] options
    #   Options for the command
    #
    #    :auto_execute  Execute directly
    # 
    #
    def self.register_processor(event, name, command_class, options)
      
      unless command_class.ancestors.index(BusinessEvents::BusinessEventCommand)
        raise ArgumentError, "#{command_class} is not a BusinessEventCommand"
      end 
           
      event = event.to_sym unless event.is_a?Symbol
      name = name.to_sym unless name.is_a?Symbol
      
      unless commands.has_key?(event)
        commands[event] = {}
      end
      
      commands[event].store(name, {:command_class => command_class, :options => options})
     
      return commands
     
    end
    
    #
    # Get the command that matches the event and name
    #
    # @param [Symbol] event
    # @param [Symbol] name
    #
    def self.get_command(event, name)
    
      event = event.to_sym unless event.is_a?Symbol
      name = name.to_sym unless name.is_a?Symbol    
    
      puts "event : #{event} name : #{name} ** #{commands}"
    
      commands.has_key?(event)?commands[event][name]:nil 
              
    end
    
    #
    # Gets the commands registered
    #
    def self.commands
      @commands
    end
   
    #
    # Find the events that have not been processed
    #
    def self.find_pending_events
    
      all()
    
    end    
         
  end #BusinessEvent
  
  #
  # Represents the status of a BusinessEvent for an specific command
  #
  class BusinessEventProcess
    include DataMapper::Resource
    
    storage_names[:default] = 'be_business_event_process'
    
    property :id, Serial, :field => 'id', :key => true
    belongs_to :business_event, 'BusinessEvent', :child_key => [:business_event_id], :parent_key => [:id]
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
  
end #BusinessEvents