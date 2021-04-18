# Validator lib - library for validating files conforming to
# https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md

require "date"
require "json"
require "uri"

US_STATES = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "AS", "GU", "MP", "PR", "VI", "UM"]

module ValidatorLib
  class Context
    # type is the type of file 'Manifest', 'Location', 'Schedule', 'Slot'.
    attr_accessor :type,
    # name is the name of the file, which could be on the local machine or a URL
      :name,
    # lineno is the line number inside the file. May be nil
      :lineno,
    # fields is an array of fields which indicates the context of the current field being validated.
      :fields
    def initialize(type, name, fields, lineno = nil)
      @type = type
      @name = name
      @fields = fields || []
      @lineno = lineno
    end

    # Returns a context string for the given subfield f in the context of @fields.
    def for_field(f)
      "#{@type} #{@name}#{@lineno.nil? ? "" : ":line-#{@lineno}"} #{field_s}.[#{f}] :"
    end

    # Returns a new context object with subfield f.
    def with_field(f)
      Context.new(@type, @name, @fields + [f], @lineno)
    end

    # Returns a context string at @fields
    def to_s
      "#{@type} #{@name}#{@lineno.nil? ? "" : ":line-#{@lineno}"} #{field_s} :"
    end

    # Returns a context object at the parent field.
    def pop
      Context.new(@type, @name, @fields.first([@fields.length - 1, 0].max), @lineno)
    end

   private
    # Builds the complete field context string
    def field_s
      ".#{@fields.map{ |f| "[#{f.is_a?(String) ? "\"#{f}\"" : f}]"}.join}"
    end
  end

  # Returns whether u is a URL.
  def is_url(u)
    url = URI.parse(u)
    url.kind_of?(URI::HTTP) or url.kind_of?(URI::HTTPS)
  end

  # If u is a URL, validates whether u ends with $bulk-publish.
  # Returns an array of validation errors.
  def validate_manifest_url_path(u)
    if is_url(u) and !(u.end_with? "$bulk-publish")
      ["Manifest url does not end with $bulk-publish [https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md#quick-start-guide]"]
    end
    []
  end

  # Validates manifest file with the given filename and contents.
  # Returns an array of validation errors.
  def validate_manifest(filename, contents)
    errors = []
    parsed = {}
    begin
      parsed = JSON.parse(contents)
    rescue JSON::ParserError => e
      return ["Failed to parse manifest #{filename}: #{e}"]
    end

    context = Context.new("Manifest", filename, [])

    errors.concat(validate_required_fields(context, ["transactionTime", "request", "output"], parsed))

    errors.concat(validate_field_types(context,
          [
            {
              name: "transactionTime",
              type: String,
            },
            {
              name: "request",
              type: String,
            },
            {
              name: "output",
              type: Array,
              error_message_type: "array of JSON objects"
            },
          ], parsed))

    errors.concat(validate_field_contents(context,
          [
            {
              name: "transactionTime",
              validator: -> (ctx, c) { iso8601_validator(ctx, c) },
            },
            {
              name: "request",
              validator: -> (ctx, c) { url_validator(ctx, c) },
            },
            {
              name: "output",
              validator: -> (ctx, c) { validate_manifest_output(ctx, c) },
            },
          ], parsed))
    errors
  end

  # Validates the manifest file's output field.
  # Returns an array of errors
  def validate_manifest_output(context, json_obj)
    return [] unless json_obj.is_a? Array
    idx = 0
    json_obj.flat_map do |output_obj|
      ctx = context.with_field(idx)
      errors = []
      errors.concat(validate_required_fields(ctx, ["type", "url"], output_obj))
      errors.concat(validate_field_types(ctx,
          [
            {name: "type", type: String},
            {name: "url", type: String},
            {name: "extension", type: Hash, error_message_type: "JSON object"},
          ], output_obj))

      errors.concat(validate_field_contents(ctx,
          [
            {
              name: "type",
              validator: -> (ctx, c) {
                one_of_strings_validator(ctx, c, ["Location", "Schedule", "Slot"])
              },
            },
            {
              name: "url",
              validator: -> (ctx, c) { url_validator(ctx, c) },
            },
            {
              name: "extension",
              validator: -> (ctx, c) { validate_manifest_output_extension(ctx, c) },
            },
          ], output_obj))
      idx += 1
      errors
    end
  end

  # Validates the manifest file's output.extension field.
  def validate_manifest_output_extension(context, json_obj)
    return [] unless json_obj.is_a? Hash
    errors = []
    errors.concat(validate_required_fields(context, ["state"], json_obj))
    errors.concat(validate_field_types(context,
        [
          {name: "state", type: Array, error_message_type: "JSON array of strings"},
        ], json_obj))
    errors.concat(validate_field_contents(context,
        [
          {
            name: "state",
            validator: -> (ctx, c) {
              idx = 0
              c.flat_map do |s|
                o = one_of_strings_validator(ctx.with_field(idx), s, US_STATES)
                idx += 1
                o
              end
            },
          },
        ], json_obj))
    errors
  end

  # Validates location file with the given filename and contents.
  # Returns an array of validation errors.
  def validate_location(filename, contents)
    errors = []
    parsed = {}
    begin
      parsed = JSON.parse(contents)
    rescue JSON::ParserError => e
      return ["Failed to parse location #{filename}: #{e}"]
    end

    context = Context.new("Location", filename, [])

    errors.concat(validate_required_fields(context,
          ["resourceType", "id", "name", "telecom", "address", "position", "identifier"], parsed))

    errors.concat(validate_field_types(context,
          [
            { name: "resourceType", type: String },
            { name: "id", type: String },
            { name: "name", type: String },
            { name: "telecom", type: Array, error_message_type: "array of JSON objects" },
            { name: "address", type: Hash, error_message_type: "JSON objects" },
            { name: "description", type: String },
            { name: "position", type: Hash, error_message_type: "JSON objects" },
            { name: "identifier", type: Array, error_message_type: "array of JSON objects" },
          ], parsed))

    errors.concat(validate_field_contents(context,
          [
            {
              name: "resourceType",
              validator: -> (ctx, c) { one_of_strings_validator(ctx, c, ["Location"]) },
            },
            {
              name: "id",
              validator: -> (ctx, c) { id_validator(ctx, c) },
            },
            {
              name: "name",
              validator: -> (ctx, c) { [] },
            },
            # XXX continue here
          ], parsed))
    errors
  end


 private
  # Validates that json_obj has fields.
  # Returns an array of validation errors.
  def validate_required_fields(context, fields, json_obj)
    return ["#{context.pop} not a JSON object"] unless json_obj.is_a? Hash
    fields.reject { |k| json_obj.has_key?(k) }.map { |k| "#{context}: missing required field #{k}" }
  end

  # Validates that json_obj's fields has the specified types.
  # want_types must be an array of hashes where each hash has the form
  # {:name => "field_name", :type => Type}
  # Optionally, include  :error_message_type => "Type to show in validation error message"
  #
  # Note missing fields are silently ignored.
  def validate_field_types(context, want_types, json_obj)
    return ["#{context.pop} not a JSON object"] unless json_obj.is_a? Hash

    want_types.
      # Remove non-existant fields.
      select { |k| json_obj.has_key?(k[:name]) }.
      # Remove fields that have the right type.
      reject do |k|
        json_obj[k[:name]].is_a? k[:type]
      end.
      # Construct error messages.
      map do |k|
        "#{context.with_field(k[:name])} field is not of type #{k[:error_message_type] || k[:type].to_s}" 
      end
  end

  # Validates that json_obj's field's contents
  # validators must be an array of hashes
  # {
  #   :name => "field_name",
  #   :validator => lambda that accepts (context, the contents of the field) and returns an array of errors
  # }
  #
  # Missing fields are ignored.
  def validate_field_contents(context, validators, json_obj)
    return ["#{context.pop} not a JSON object"] unless json_obj.is_a? Hash

    validators.
      # Remove non-existant fields
      select { |k| json_obj.has_key?(k[:name]) }.
      # Call validators.
      flat_map { |k| k[:validator].call(context.with_field(k[:name]), json_obj[k[:name]]) }
  end

  def url_validator(context, c)
    return [] if is_url(c)
    ["#{context} is not a URL. Got '#{c}'"]
  end

  def iso8601_validator(context, c)
    begin
      DateTime.iso8601(c)
    rescue Date::Error => e
      return ["#{context} cannot be parsed as a ISO8601 timestamp. Got '#{c}'"]
    end
    []
  end

  def one_of_strings_validator(context, c, opts)
    return [] if opts.include? c
    ["#{context} unrecognized field value '#{c}'. Must be one of #{opts.inspect}"]
  end

  def id_validator(context, c)
    return [] unless string.match?(/[^[:alnum:]_.]/) 
    ["#{context} id contains forbidden characters (only alphanumeric and '_' , '.' allowed)"]
  end
end
