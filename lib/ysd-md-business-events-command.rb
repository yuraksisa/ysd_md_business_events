module BusinessEvents

  class BusinessEventCommand
  
    attr_reader :business_event
   
    def initialize(business_event)
      @business_event = business_event
    end
  
    def execute
      raise NotImplementedError, "#{self.class}#execute not implemented"
    end
  
  end

end