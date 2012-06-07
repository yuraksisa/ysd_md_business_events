require 'data_mapper'
require 'ysd-md-business_events'
require 'ysd_core_incompatibilities'

# DataMapper configuration

DataMapper::Logger.new($stdout, :debug)
DataMapper.setup(:default, { :adapter => 'postgres', :database => 'me_entiendes', :host => '192.168.1.133', :username => 'development', :password => 'developer' })
DataMapper.finalize
DataMapper::Model.raise_on_save_failure = true 

DataMapper.auto_upgrade! #auto_migrate!

# Define a command that returns the data

class EchoBusinessEventCommand < BusinessEvents::BusinessEventCommand
  def execute
    business_event.data
  end
end

class NoneBusinessEventCommand < BusinessEvents::BusinessEventCommand
  def execute
    "none"
  end
end

#class EchoBusinessEventCommand < BusinessEvents::BusinessEventCommand; def execute; data; end; end

#class NoneBusinessEventCommand < BusinessEvents::BusinessEventCommand; def execute; "none"; end; end

# Register the command
BusinessEvents::BusinessEvent.register_processor(:echo, :basic, EchoBusinessEventCommand)
BusinessEvents::BusinessEvent.register_processor(:echo, :none, NoneBusinessEventCommand)

# Create an event (It will create the event and two processors)
my_event = BusinessEvents::BusinessEvent.fire_event(:echo, {:name => 'Juan'})

# Process the events (by the processors)
my_event.process
