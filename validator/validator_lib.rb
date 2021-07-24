# Validator lib - library for validating files conforming to
# https://github.com/smart-on-fhir/smart-scheduling-links/blob/master/specification.md

require "date"
require "json"
require "uri"
require "set"

US_STATES = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "AS", "GU", "MP", "PR", "VI", "UM"]

module ValidatorLib

  # Context is a class containing information about the location of the JSON field being validated.
  # It is printed as part of validation error messages in order to help 
  # users understand where the error occurred.
  # The context string uses the same query language as the `jq` tool.
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

    # Returns a context string.
    def to_s
      "#{@type} #{@name}#{line_s} #{field_s} "
    end

    # Returns a context object with subfield f.
    def with_field(f)
      Context.new(@type, @name, @fields + [f], @lineno)
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

    # Builds a line number string, if lineno is not nil. "" otherwise.
    def line_s
      @lineno.nil? ? "" : ":line-#{@lineno}"
    end
  end

  # State tracks the current state of the validator
  # Used to validate rules that apply across object boundaries,
  # e.g. "id must be unique in all locations".
  class State
    attr_accessor :location_ids,
      :schedule_ids,
      :slot_ids
    # X_ids are an array of hashes.
    # {
    #   id: "id string"
    #   context: Context object where the id was specified.
    # }

    # Id references made
    attr_accessor :location_id_refs,
      :schedule_id_refs
    # X_id_refs are an array of hashes.
    # {
    #   id: "id reference"
    #   context: Context object where the reference was made.
    # }

    def initialize()
      @location_ids = []
      @schedule_ids = []
      @slot_ids = []

      @location_id_refs = []
      @schedule_id_refs = []
    end

    # add_id adds id to the list of known ids.
    def add_id(context, id_type, id)
      new_id = {id: id, context: context.clone}

      case id_type
      when "location_id"
        @location_ids << new_id
      when "schedule_id"
        @schedule_ids << new_id
      when "slot_id"
        @slot_ids << new_id
      else
        raise "Unkonwn id type #{id_type}"
      end
    end

    # add_id_ref adds id-ref to the list of id_refs.
    # note that id_type refers to the type of id being referred to (as opposed to the referree).
    def add_id_ref(context, id_type, id)
      new_id_ref = {id: id, context: context.clone}
      case id_type
      when "location_id"
        @location_id_refs << new_id_ref
      when "schedule_id"
        @schedule_id_refs << new_id_ref
      else
        raise "Unkonwn id ref type #{id_type}"
      end
    end

    # errors validates all ids and id references in state and returns a list of errors.
    def errors
      duplicate_finder = -> (name, ids) {
        ids.group_by { |i| i[:id] }.
        reject { |k, v| v.length == 1 }.
        map do |k, v|
          "#{name} id '#{k}' duplicated [#{v.map { |id| id[:context].to_s }.join(", ")}]"
        end
      }

      invalid_ref_finder = -> (name, ids, refs) {
        ids = Set.new(ids.map { |i| i[:id] })

        refs.reject { |r| ids.include?(r[:id]) }.map do |r|
          "Unknown #{name} id '#{r[:id]}' referenced by #{r[:context]}"
        end
      }

      errors = []
      errors.concat(duplicate_finder.call("Location", @location_ids))
      errors.concat(duplicate_finder.call("Schedule", @schedule_ids))
      errors.concat(duplicate_finder.call("Slot", @slot_ids))

      errors.concat(duplicate_finder.call("location", @location_id_refs))
      errors.concat(duplicate_finder.call("schedule", @schedule_id_refs))

      errors
    end
  end

  # Returns whether u is a URL.
  def is_url(u)
    return false if u.nil?
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

  # Returns resources named by manifest file.
  # manifest_contents: string
  # If manifest_contents cannot be parsed, an empty array is returned.
  # Returns an array of hashes, each hash:
  # {
  #   :type => "Location" or "Schedule" or "Slot"
  #   :url => url of the file
  # }
  # Unknown types are dropped.
  def manifest_resources(manifest_contents)
    parsed = {}
    begin
      parsed = JSON.parse(manifest_contents)
    rescue JSON::ParserError => e
      return []
    end

    parsed["output"]&.map do |obj|
      unless obj&.is_a? Hash and obj.has_key?("type") and obj.has_key?("url")
        next nil
      end
      next nil unless ["Location", "Schedule", "Slot"].include? obj["type"]

      {
        type: obj["type"],
        url: obj["url"],
      }
    end.reject(&:nil?) || []
  end

  # Validates manifest file with the given filename and contents.
  # Returns an array of validation errors.
  def validate_manifest(state, filename, contents)
    errors = []
    parsed = {}
    begin
      parsed = JSON.parse(contents)
    rescue JSON::ParserError => e
      return ["Failed to parse manifest #{filename}: #{e}"]
    end

    context = Context.new("Manifest", filename, [])

    validate_json_object(
      context, state, parsed,
      required_fields: ["transactionTime", "request", "output"],
      field_types: [
        {name: "transactionTime", type: String},
        {name: "request", type: String},
        {name: "output", type: Array},
      ],
      field_contents: [
        {
          name: "transactionTime",
          validator: -> (ctx, state, c) { iso8601_validator(ctx, c) },
        },
        {
          name: "request",
          validator: -> (ctx, state, c) { url_validator(ctx, c) },
        },
        {
          name: "output",
          validator: -> (ctx, state, c) { validate_manifest_output(ctx, state, c) },
        },
      ],
      object_validator: nil)
  end

  # Validates the manifest file's output field.
  # Returns an array of errors
  def validate_manifest_output(context, state, json_obj)
    validate_json_array(
      context, state, json_obj,
      required_fields: ["type", "url"],
      field_types: [
          {name: "type", type: String},
          {name: "url", type: String},
          {name: "extension", type: Hash},
      ],
      field_contents: [
        {
          name: "type",
          validator: -> (ctx, state, c) {
            one_of_strings_validator(ctx, c, ["Location", "Schedule", "Slot"])
          },
        },
        {
          name: "url",
          validator: -> (ctx, state, c) { url_validator(ctx, c) },
        },
        {
          name: "extension",
          validator: -> (ctx, state, c) { validate_manifest_output_extension(ctx, state, c) },
        },
      ],
      object_validator: nil)
  end

  # Validates the manifest file's output.extension field.
  def validate_manifest_output_extension(context, state, json_obj)
    validate_json_object(
      context, state, json_obj,
      required_fields: ["state"],
      field_types: [
        {name: "state", type: Array, error_message_type: "JSON array of strings"},
      ],
      field_contents: [
        {
          name: "state",
          validator: -> (ctx, state, c) {
            c.each_with_index.flat_map do |s, idx|
              if s.is_a? String
                one_of_strings_validator(ctx.with_field(idx), s, US_STATES)
              else
                ["#{ctx.with_field(idx)}: not a string. Got '#{s}'"]
              end
            end
          },
        },
      ],
      object_validator: nil)
  end

  # Validates location file with the given filename and contents.
  # Returns an array of validation errors.
  def validate_location(state, filename, contents)
    errors = contents.split("\n").each_with_index.flat_map do |co, idx|
      errors = []
      parsed = {}
      begin
        parsed = JSON.parse(co)
      rescue JSON::ParserError => e
        next ["Failed to parse location #{filename}: #{e}"]
      end

      context = Context.new("Location", filename, [], idx + 1)

      validate_json_object(
        context, state, parsed,
        required_fields: ["resourceType", "id", "name", "telecom", "address", "identifier"],
        field_types: [
          { name: "resourceType", type: String },
          { name: "id", type: String },
          { name: "name", type: String },
          { name: "telecom", type: Array },
          { name: "address", type: Hash },
          { name: "description", type: String },
          { name: "position", type: Hash },
          { name: "identifier", type: Array },
        ],
        field_contents: [
          {
            name: "resourceType",
            validator: -> (ctx, state, c) { one_of_strings_validator(ctx, c, ["Location"]) },
          },
          {
            name: "id",
            validator: -> (ctx, state, c) { id_validator(ctx, state, "location_id", c) },
          },
          {
            name: "telecom",
            validator: -> (ctx, state, c) { validate_location_telecom(ctx, state, c) },
          },
          {
            name: "address",
            validator: -> (ctx, state, c) { validate_location_address(ctx, state, c) },
          },
          {
            name: "position",
            validator: -> (ctx, state, c) { validate_location_position(ctx, state, c) },
          },
          {
            name: "identifier",
            validator: -> (ctx, state, c) { validate_location_identifier(ctx, state, c) },
          },
        ],
        object_validator: nil)
    end
  end

  def validate_location_telecom(context, state, json_obj)
    validate_json_array(
      context, state, json_obj,
      required_fields: ["system", "value"],
      field_types: [
          {name: "system", type: String},
          {name: "value", type: String},
      ],
      field_contents: [
        {
          name: "system",
          validator: -> (ctx, state, c) {
            one_of_strings_validator(ctx, c, ["phone", "url"])
          },
        },
      ],
      object_validator: -> (ctx, state, c) {
        errors = []
        if c["system"] == "phone"
          errors.concat(phone_validator(ctx.with_field("value"), c["value"]))
        elsif c["system"] == "url"
          errors.concat(url_validator(ctx.with_field("value"), c["value"]))
        end
        errors
      })
  end

  def validate_location_address(context, state, json_obj)
    validate_json_object(
      context, state, json_obj,
      required_fields: ["line", "city", "state", "postalCode"],
      field_types: [
        {name: "line", type: Array, error_message_type: "JSON array of strings"},
        {name: "city", type: String},
        {name: "state", type: String},
        {name: "postalCode", type: String},
        {name: "district", type: String},
      ],
      field_contents: [
        {
          name: "line",
          validator: -> (ctx, state, c) {
            return [] unless c.is_a? Array
            c.each_with_index.flat_map do |s, idx|
              if s.is_a? String
                []
              else
                ["#{ctx.with_field(idx)}: not a string. Got '#{s}'"]
              end
            end
          },
        },
      ],
      object_validator: nil)
  end

  def validate_location_position(context, state, json_obj)
    validate_json_object(
      context, state, json_obj,
      required_fields: ["latitude", "longitude"],
      field_types: [
        {name: "latitude", type: Numeric},
        {name: "longitude", type: Numeric},
      ],
      field_contents: [],
      object_validator: nil)
  end

  def validate_location_identifier(context, state, json_obj)
    validate_json_array(
      context, state, json_obj,
      required_fields: ["system", "value"],
      field_types: [
          {name: "system", type: String},
          {name: "value", type: String},
      ],
      field_contents: [],
      object_validator: nil)
  end

  # Validates schedule file with the given filename and contents.
  # Returns an array of validation errors.
  def validate_schedule(state, filename, contents)
    contents.split("\n").each_with_index.flat_map do |co, idx|
      errors = []
      parsed = {}
      begin
        parsed = JSON.parse(co)
      rescue JSON::ParserError => e
        return ["Failed to parse schedule #{filename}: #{e}"]
      end

      context = Context.new("Schedule", filename, [], idx + 1)

      validate_json_object(
        context, state, parsed,
        required_fields: ["resourceType", "id", "actor", "serviceType"],
        field_types: [
          { name: "resourceType", type: String },
          { name: "id", type: String },
          { name: "actor", type: Array },
          { name: "serviceType", type: Array },
          { name: "extension", type: Array },
        ],
        field_contents: [
          {
            name: "resourceType",
            validator: -> (ctx, state, c) { one_of_strings_validator(ctx, c, ["Schedule"]) },
          },
          {
            name: "id",
            validator: -> (ctx, state, c) { id_validator(ctx, state, "schedule_id", c) },
          },
          {
            name: "actor",
            validator: -> (ctx, state, c) { validate_schedule_actor(ctx, state, c) },
          },
          {
            name: "serviceType",
            validator: -> (ctx, state, c) { validate_schedule_service_type(ctx, state, c) },
          },
          {
            name: "extension",
            validator: -> (ctx, state, c) { validate_schedule_extension(ctx, state, c) },
          },
        ],
        object_validator: nil)
    end
  end

  def validate_schedule_actor(context, state, json_obj)
    return ["#{context}: actor must have only one JSON object. Got #{json_obj.length} objects."] unless json_obj.length == 1
    validate_json_array(
      context, state, json_obj,
      required_fields: ["reference"],
      field_types: [{name: "reference", type: String}],
      field_contents: [
        {
          name: "reference",
          validator: -> (ctx, state, c) { id_ref_validator(ctx, state, "location_id", c) },
        },
      ],
      object_validator: nil)
  end

  def validate_schedule_service_type(context, state, json_obj)
    validate_json_array(
      context, state, json_obj,
      required_fields: ["coding"],
      field_types: [
        {name: "coding", type: Array},
      ],
      field_contents: [
        {
          name: "coding",
          validator: -> (ctx, state, c) { validate_schedule_service_type_coding(ctx, state, c) },
        },
      ],
      object_validator: nil)
  end

  def validate_schedule_service_type_coding(context, state, json_obj)
    errors = validate_json_array(
      context, state, json_obj,
      required_fields: ["system", "code", "display"],
      field_types: [
        {name: "system", type: String},
        {name: "code", type: String},
        {name: "display", type: String},
      ],
      field_contents: [],
      object_validator: nil)

    return errors unless json_obj.is_a? Array

    # Should these be considered a validation error?
    unless json_obj.find do |obj|
        (obj["system"] == "http://terminology.hl7.org/CodeSystem/service-type") and
          (obj["code"] == "57") and
          (obj["display"] == "Immunization")
      end
      errors << %Q(#{context}: Schedule file must have serviceType with JSON object '{ "system": "http://terminology.hl7.org/CodeSystem/service-type", "code": "57", "display": "Immunization" }')
    end

    # Should this be considered a validation error?
    unless json_obj.find do |obj|
        (obj["system"] == "http://fhir-registry.smarthealthit.org/CodeSystem/service-type") and
          (obj["code"] == "covid19-immunization") and
          (obj["display"] == "COVID-19 Immunization Appointment")
      end
      errors << %Q(#{context}: Schedule file must have serviceType with JSON object '{ "system": "http://fhir-registry.smarthealthit.org/CodeSystem/service-type", "code": "covid19-immunization", "display": "COVID-19 Immunization Appointment" }')
    end

    errors
  end

  def validate_schedule_extension(context, state, json_obj)
    validate_json_array(
      context, state, json_obj,
      required_fields: ["url"],
      field_types: [
        {name: "url", type: String},
        {name: "valueInteger", type: Numeric},
      ],
      field_contents: [
        {
          name: "url",
          validator: -> (ctx, state, c) {
            one_of_strings_validator(
              ctx, c,
              [
                "http://fhir-registry.smarthealthit.org/StructureDefinition/vaccine-product",
                "http://fhir-registry.smarthealthit.org/StructureDefinition/vaccine-dose",
                "http://fhir-registry.smarthealthit.org/StructureDefinition/has-availability",
              ]
            )
          }
        },
      ],
      object_validator: -> (ctx, state, c) {
        errors = []

        if c["url"] == "http://fhir-registry.smarthealthit.org/StructureDefinition/vaccine-product"
          u = "http://fhir-registry.smarthealthit.org/StructureDefinition/vaccine-product"
          validate_json_object(
            ctx.with_field("valueCoding"), state, c["valueCoding"],
            required_fields: ["system", "code", "display"],
            field_types: [
              {name: "system", type: String},
              {name: "code", type: String},
              {name: "display", type: String},
            ],
            field_contents: [
              {
                name: "system",
                validator: -> (ctx, state, c) { one_of_strings_validator(ctx, c, ["http://hl7.org/fhir/sid/cvx"]) },
              },
              {
                name: "code",
                validator: -> (ctx, state, c) { one_of_strings_validator(ctx, c, ["207", "208", "210", "212"]) },
              },
            ],
            object_validator: nil
          )
        elsif c["url"] == "http://fhir-registry.smarthealthit.org/StructureDefinition/vaccine-dose"
          unless c["valueInteger"].is_a? Numeric
            errors << "#{ctx}: extension with url 'http://fhir-registry.smarthealthit.org/StructureDefinition/vaccine-dose' must have an associated number valueInteger field"
          end
        elsif c["url"] == "http://fhir-registry.smarthealthit.org/StructureDefinition/has-availability"
          unless ["some", "none", "unknown"].include? c["valueCode"]
            errors << "#{ctx}: extension with url 'http://fhir-registry.smarthealthit.org/StructureDefinition/has-availability' must have one of 'some', 'none', 'unknown' valueCode"
          end
        end
        errors
      })
  end

  # Validates slot file with the given filename and contents.
  # Returns an array of validation errors.
  def validate_slot(state, filename, contents)
    contents.split("\n").each_with_index.flat_map do |co, idx|
      errors = []
      parsed = {}
      begin
        parsed = JSON.parse(co)
      rescue JSON::ParserError => e
        return ["Failed to parse slot #{filename}: #{e}"]
      end

      context = Context.new("Slot", filename, [], idx + 1)

      validate_json_object(
        context, state, parsed,
        required_fields: ["resourceType", "id", "schedule", "status", "start", "end"],
        field_types: [
          { name: "resourceType", type: String },
          { name: "id", type: String },
          { name: "schedule", type: Hash },
          { name: "status", type: String },
          { name: "start", type: String },
          { name: "end", type: String },
          { name: "extension", type: Array },
        ],
        field_contents: [
          {
            name: "resourceType",
            validator: -> (ctx, state, c) { one_of_strings_validator(ctx, c, ["Slot"]) },
          },
          {
            name: "id",
            validator: -> (ctx, state, c) { id_validator(ctx, state, "slot_id", c) },
          },
          {
            name: "schedule",
            validator: -> (ctx, state, c) { validate_slot_schedule(ctx, state, c) },
          },
          {
            name: "status",
            validator: -> (ctx, state, c) { one_of_strings_validator(ctx, c, ["free", "busy"]) },
          },
          {
            name: "start",
            validator: -> (ctx, state, c) { iso8601_validator(ctx, c) },
          },
          {
            name: "end",
            validator: -> (ctx, state, c) { iso8601_validator(ctx, c) },
          },
          {
            name: "extension",
            validator: -> (ctx, state, c) { validate_slot_extension(ctx, state, c) },
          },
        ],
        object_validator: nil)
    end
  end

  def validate_slot_schedule(context, state, json_obj)
    validate_json_object(
      context, state, json_obj,
      required_fields: ["reference"],
      field_types: [{name: "reference", type: String}],
      field_contents: [
        {
          name: "reference",
          validator: -> (ctx, state, c) { id_ref_validator(ctx, state, "schedule_id", c) },
        },
      ],
      object_validator: nil)
  end

  def validate_slot_extension(context, state, json_obj)
    validate_json_array(
      context, state, json_obj,
      required_fields: ["url"],
      field_types: [
        {name: "url", type: String},
        {name: "valueUrl", type: String},
        {name: "valueString", type: String},
        {name: "valueInteger", type: Numeric},
      ],
      field_contents: [
        {
          name: "url",
          validator: -> (ctx, state, c) {
            one_of_strings_validator(
              ctx, c, [
                "http://fhir-registry.smarthealthit.org/StructureDefinition/booking-deep-link",
                "http://fhir-registry.smarthealthit.org/StructureDefinition/booking-phone",
                "http://fhir-registry.smarthealthit.org/StructureDefinition/slot-capacity",
              ]
            )
          },
        },
        {
          name: "valueUrl",
          validator: -> (ctx, state, c) { url_validator(ctx, c) },
        },
        {
          name: "valueString",
          validator: -> (ctx, state, c) { phone_validator(ctx, c) },
        },
      ],
      object_validator: -> (ctx, state, c) {
        case c["url"]
        when "http://fhir-registry.smarthealthit.org/StructureDefinition/booking-deep-link"
          if c["valueUrl"].nil?
            return ["#{context}: extension type 'http://fhir-registry.smarthealthit.org/StructureDefinition/booking-deep-link' must have an associated valueUrl specified"]
          end
        when "http://fhir-registry.smarthealthit.org/StructureDefinition/booking-phone"
          if c["valueString"].nil?
            return ["#{context}: extension type 'http://fhir-registry.smarthealthit.org/StructureDefinition/booking-phone' must have an associated valueString specified"]
          end
        when "http://fhir-registry.smarthealthit.org/StructureDefinition/slot-capacity"
          if c["valueInteger"].nil?
            return ["#{context}: extension type 'http://fhir-registry.smarthealthit.org/StructureDefinition/slot-capacity' must have an associated valueInteger specified"]
          end
        end
        []
      })
  end
 private
  # Validates that json_obj has fields.
  # Returns an array of validation errors.
  def validate_required_fields(context, fields, json_obj)
    fields.reject { |k| json_obj.has_key?(k) }.map { |k| "#{context}: missing required field #{k}" }
  end

  # Validates that json_obj's fields has the specified types.
  # want_types must be an array of hashes where each hash has the form
  # {:name => "field_name", :type => Type}
  # Optionally, include  :error_message_type => "Type to show in validation error message"
  #
  # Note missing fields are silently ignored.
  def validate_field_types(context, want_types, json_obj)
    want_types.
      # Remove non-existant fields.
      select { |k| json_obj.has_key?(k[:name]) }.
      # Remove fields that have the right type.
      reject do |k|
        json_obj[k[:name]].is_a? k[:type]
      end.
      # Construct error messages.
      map do |k|
        type_name = k[:error_message_type] || (
          if k[:type] == Hash
            "JSON object"
          elsif k[:type] == Array
            "array of JSON objects"
          elsif k[:type] == Numeric
            "number"
          else
            k[:type].to_s
          end
        )
        "#{context.with_field(k[:name])}: field is not of type #{type_name}" 
      end
  end

  # Validates that json_obj's field's contents
  # validators must be an array of hashes
  # {
  #   :name => "field_name",
  #   :validator => lambda that accepts (context, state, the contents of the field) and returns an array of errors
  # }
  #
  # Missing fields are ignored.
  def validate_field_contents(context, state, validators, json_obj)
    validators.
      # Remove non-existant fields
      select { |k| json_obj.has_key?(k[:name]) }.
      # Call validators.
      flat_map { |k| k[:validator].call(context.with_field(k[:name]), state, json_obj[k[:name]]) }
  end

  # Validates that c is an array of JSON objects. Each object is then 
  # passed to validate_json_object. See validate_json_object for arugment descriptions.
  # object_validator is applied to each object in the array.
  #
  # All validators are skipped if c is not an Array.
  #
  # returns a list of errors [String]
  def validate_json_array(
    context, state, c,
    required_fields:,
    field_types:,
    field_contents:,
    object_validator:
  )
    return ["#{context}: is not a JSON array. Got '#{c}'"] unless c.is_a? Array
    return ["#{context}: JSON array cannot be empty. Omit field instead if field is optional."] if c.empty?
    c.each_with_index.flat_map do |co, idx|
      ctx = context.with_field(idx)
      validate_json_object(
        ctx, state, co,
        required_fields: required_fields, field_types: field_types,
        field_contents: field_contents, object_validator: object_validator)
    end
  end

  # Validates that JSON object c has required_fields, with field_types and has field_contents.
  # Optionally an object_validator may be passed to validate the entire object.
  #
  # Field type and field content validators are skipped if the field is missing.
  # All validators are skipped if c is not a Hash.
  #
  # context Context
  # state State
  # c Object
  # required_fields [String]
  # field_types [{name: String, type: Class, (optionally) error_message_type: string}]
  # field_contents [{name: String, validator: lambda (context, state, field_contents) -> [String]}]
  # object_validator: (optional) lambda (context, state, object) -> [String]
  # returns a list of errors [String]
  def validate_json_object(
    context, state, c,
    required_fields:,
    field_types:,
    field_contents:,
    object_validator:
  )
    return ["#{context}: is not a JSON object. Got '#{c}'"] unless c.is_a? Hash
    errors = validate_required_fields(context, required_fields, c)
    errors.concat(validate_field_types(context, field_types, c))
    errors.concat(validate_field_contents(context, state, field_contents, c))
    unless object_validator.nil?
      errors.concat(object_validator.call(context, state, c))
    end
    errors
  end

  def url_validator(context, c)
    return [] if is_url(c)
    ["#{context}: is not a URL. Got '#{c}'"]
  end

  def phone_validator(context, c)
    # TODO: improve phone validator.
    return [] if c.nil? or c.match?(/[0-9\-()]+/)
    ["#{context}: is not a phone number. Got '#{c}'"]
  end

  def iso8601_validator(context, c)
    begin
      DateTime.iso8601(c)
    rescue Date::Error => e
      return ["#{context}: cannot be parsed as a ISO8601 timestamp. Got '#{c}'"]
    end
    []
  end

  def one_of_strings_validator(context, c, opts)
    return [] if opts.include? c
    ["#{context}: unrecognized field value '#{c}'. Must be one of #{opts.inspect}"]
  end

  def id_validator(context, state, id_type, c)
    errors = []
    if c.match?(/[^[:alnum:]_.]/) 
      errors << "#{context}: id contains forbidden characters (only alphanumeric and '_' , '.' allowed)"
    end

    state.add_id(context, id_type, c)
    []
  end

  def id_ref_validator(context, state, id_reference_type, c)
    case id_reference_type
    when "location_id"
      md = c.match(/Location\/(.)+/)
      if md.nil?
        return ["#{context}: location id reference does not have the form 'Location/' + id"]
      else
        state.add_id_ref(context, "location_id", md[1])
        return []
      end
    when "schedule_id"
      md = c.match(/Schedule\/(.)+/)
      if md.nil?
        return ["#{context}: schedule id reference does not have the form 'Schedule/' + id"]
      else
        state.add_id_ref(context, "schedule_id", md[1])
        return []
      end
    else
      raise "Unknown id reference type: #{id_reference_type}"
    end
  end
end
