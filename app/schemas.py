import json
from jsonschema import validate
import jsonschema
from .datasetmodel import Dataset,SchemaValidation


schema = {
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "properties": {
    "id": {
      "type": "string"
    },
    "dataset_id": {
      "type": "string"
    },
    "type": {
      "type": "string"
    },
    "name": {
      "type": "string"
    },
    "validation_config": {
      "type": "object",
      "properties": {
        "validate": {
          "type": "boolean"
        },
        "mode": {
          "type": "string"
        },
        "validation_mode": {
          "type": "string"
        }
      },
      "required": [
        "validate",
        "mode",
        "validation_mode"
      ]
    },
    "extraction_config": {
      "type": "object",
      "properties": {
        "is_batch_event": {
          "type": "boolean"
        },
        "extraction_key": {
          "type": "string"
        },
        "dedup_config": {
          "type": "object",
          "properties": {
            "drop_duplicates": {
              "type": "boolean"
            },
            "dedup_key": {
              "type": "string"
            },
            "dedup_period": {
              "type": "integer"
            }
          },
          "required": [
            "drop_duplicates",
            "dedup_key",
            "dedup_period"
          ]
        },
        "batch_id": {
          "type": "string"
        }
      },
      "required": [
        "is_batch_event",
        "extraction_key",
        "dedup_config",
        "batch_id"
      ]
    },
    "dedup_config": {
      "type": "object",
      "properties": {
        "drop_duplicates": {
          "type": "boolean"
        },
        "dedup_key": {
          "type": "string"
        },
        "dedup_period": {
          "type": "integer"
        }
      },
      "required": [
        "drop_duplicates",
        "dedup_key",
        "dedup_period"
      ]
    },
    "data_schema": {
      "type": "object",
      "properties": {
        "$schema": {
          "type": "string"
        },
        "title": {
          "type": "string"
        },
        "description": {
          "type": "string"
        },
        "type": {
          "type": "string"
        },
        "properties": {
          "type": "object",
          "properties": {
            "obsCode": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                }
              },
              "required": [
                "type"
              ]
            },
            "codeComponents": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                },
                "items": {
                  "type": "object",
                  "properties": {
                    "type": {
                      "type": "string"
                    },
                    "properties": {
                      "type": "object",
                      "properties": {
                        "componentCode": {
                          "type": "object",
                          "properties": {
                            "type": {
                              "type": "string"
                            }
                          },
                          "required": [
                            "type"
                          ]
                        },
                        "componentType": {
                          "type": "object",
                          "properties": {
                            "type": {
                              "type": "string"
                            },
                            "enum": {
                              "type": "array",
                              "items": [
                                {
                                  "type": "string"
                                },
                                {
                                  "type": "string"
                                },
                                {
                                  "type": "string"
                                },
                                {
                                  "type": "string"
                                },
                                {
                                  "type": "string"
                                },
                                {
                                  "type": "string"
                                },
                                {
                                  "type": "string"
                                },
                                {
                                  "type": "string"
                                },
                                {
                                  "type": "string"
                                },
                                {
                                  "type": "string"
                                },
                                {
                                  "type": "string"
                                },
                                {
                                  "type": "string"
                                }
                              ]
                            }
                          },
                          "required": [
                            "type",
                            "enum"
                          ]
                        },
                        "selector": {
                          "type": "object",
                          "properties": {
                            "type": {
                              "type": "string"
                            }
                          },
                          "required": [
                            "type"
                          ]
                        },
                        "value": {
                          "type": "object",
                          "properties": {
                            "type": {
                              "type": "string"
                            }
                          },
                          "required": [
                            "type"
                          ]
                        },
                        "valueUoM": {
                          "type": "object",
                          "properties": {
                            "type": {
                              "type": "string"
                            }
                          },
                          "required": [
                            "type"
                          ]
                        }
                      },
                      "required": [
                        "componentCode",
                        "componentType",
                        "selector",
                        "value",
                        "valueUoM"
                      ]
                    }
                  },
                  "required": [
                    "type",
                    "properties"
                  ]
                }
              },
              "required": [
                "type",
                "items"
              ]
            },
            "valueUoM": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                }
              },
              "required": [
                "type"
              ]
            },
            "value": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                }
              },
              "required": [
                "type"
              ]
            },
            "id": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                }
              },
              "required": [
                "type"
              ]
            },
            "parentCollectionRef": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                }
              },
              "required": [
                "type"
              ]
            },
            "integrationAccountRef": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                }
              },
              "required": [
                "type"
              ]
            },
            "assetRef": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                }
              },
              "required": [
                "type"
              ]
            },
            "xMin": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                }
              },
              "required": [
                "type"
              ]
            },
            "xMax": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                }
              },
              "required": [
                "type"
              ]
            },
            "yMin": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                }
              },
              "required": [
                "type"
              ]
            },
            "yMax": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                }
              },
              "required": [
                "type"
              ]
            },
            "phenTime": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                },
                "format": {
                  "type": "string"
                },
                "suggestions": {
                  "type": "array",
                  "items": [
                    {
                      "type": "object",
                      "properties": {
                        "message": {
                          "type": "string"
                        },
                        "advice": {
                          "type": "string"
                        },
                        "resolutionType": {
                          "type": "string"
                        },
                        "severity": {
                          "type": "string"
                        }
                      },
                      "required": [
                        "message",
                        "advice",
                        "resolutionType",
                        "severity"
                      ]
                    }
                  ]
                }
              },
              "required": [
                "type",
                "format",
                "suggestions"
              ]
            },
            "phenEndTime": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                },
                "format": {
                  "type": "string"
                },
                "suggestions": {
                  "type": "array",
                  "items": [
                    {
                      "type": "object",
                      "properties": {
                        "message": {
                          "type": "string"
                        },
                        "advice": {
                          "type": "string"
                        },
                        "resolutionType": {
                          "type": "string"
                        },
                        "severity": {
                          "type": "string"
                        }
                      },
                      "required": [
                        "message",
                        "advice",
                        "resolutionType",
                        "severity"
                      ]
                    }
                  ]
                }
              },
              "required": [
                "type",
                "format",
                "suggestions"
              ]
            },
            "spatialExtent": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                }
              },
              "required": [
                "type"
              ]
            },
            "modified": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                }
              },
              "required": [
                "type"
              ]
            }
          },
          "required": [
            "obsCode",
            "codeComponents",
            "valueUoM",
            "value",
            "id",
            "parentCollectionRef",
            "integrationAccountRef",
            "assetRef",
            "xMin",
            "xMax",
            "yMin",
            "yMax",
            "phenTime",
            "phenEndTime",
            "spatialExtent",
            "modified"
          ]
        },
        "required": {
          "type": "array",
          "items": [
            {
              "type": "string"
            },
            {
              "type": "string"
            },
            {
              "type": "string"
            },
            {
              "type": "string"
            },
            {
              "type": "string"
            },
            {
              "type": "string"
            }
          ]
        }
      },
      "required": [
        "$schema",
        "title",
        "description",
        "type",
        "properties",
        "required"
      ]
    },
    "denorm_config": {
      "type": "object",
      "properties": {
        "redis_db_host": {
          "type": "string"
        },
        "redis_db_port": {
          "type": "integer"
        },
        "denorm_fields": {
          "type": "array",
          "items": [
            {
              "type": "object",
              "properties": {
                "denorm_key": {
                  "type": "string"
                },
                "redis_db": {
                  "type": "integer"
                },
                "denorm_out_field": {
                  "type": "string"
                }
              },
              "required": [
                "denorm_key",
                "redis_db",
                "denorm_out_field"
              ]
            },
            {
              "type": "object",
              "properties": {
                "denorm_key": {
                  "type": "string"
                },
                "redis_db": {
                  "type": "integer"
                },
                "denorm_out_field": {
                  "type": "string"
                }
              },
              "required": [
                "denorm_key",
                "redis_db",
                "denorm_out_field"
              ]
            }
          ]
        }
      },
      "required": [
        "redis_db_host",
        "redis_db_port",
        "denorm_fields"
      ]
    },
    "router_config": {
      "type": "object",
      "properties": {
        "topic": {
          "type": "string"
        }
      },
      "required": [
        "topic"
      ]
    },
    "dataset_config": {
      "type": "object",
      "properties": {
        "data_key": {
          "type": "string"
        },
        "timestamp_key": {
          "type": "string"
        },
        "exclude_fields": {
          "type": "array",
          "items": {}
        },
        "entry_topic": {
          "type": "string"
        },
        "redis_db_host": {
          "type": "string"
        },
        "redis_db_port": {
          "type": "integer"
        },
        "index_data": {
          "type": "boolean"
        },
        "redis_db": {
          "type": "integer"
        }
      },
      "required": [
        "data_key",
        "timestamp_key",
        "exclude_fields",
        "entry_topic",
        "redis_db_host",
        "redis_db_port",
        "index_data",
        "redis_db"
      ]
    },
    "status": {
      "type": "string"
    },
    "tags": {
      "type": "array",
      "items": {}
    },
    "data_version": {
      "type": "integer"
    },
    "created_by": {
      "type": "string"
    },
    "updated_by": {
      "type": "string"
    }
  },
  "required": [
    "id",
    "dataset_id",
    "type",
    "name",
    "validation_config",
    "extraction_config",
    "dedup_config",
    "data_schema",
    "denorm_config",
    "router_config",
    "dataset_config",
    "status",
    "tags",
    "data_version",
    "created_by",
    "updated_by"
  ]
}

def validate_request(dataset: SchemaValidation):
    try: 
        validate(instance=dataset.__dict__ ,schema=schema)
    except jsonschema.exceptions.ValidationError as error:
        print(error.message)
        return False
    
    return True


