// ValenceDeletionStep — one row in the deletion DAG (see `valence-platform` crate docs).

use ::valence::prelude::*;
use ::valence::privacy_policies::common::SYSTEM_ONLY;


valence_schema! {
    ValenceDeletionStep {
        table: "valence_deletion_step",
        version: "0.1.0",
        database: crate::DEFAULT_PLATFORM_STORAGE,
        description: "One deletion DAG node (one record to process in a run)",

        privacy: {
            gdpr_compliant: false,
        },

        policies: {
            read: {
                always_allow: [],
                allow: [SYSTEM_ONLY],
                block: [],
                always_block: [],
            },
            create: {
                always_allow: [],
                allow: [SYSTEM_ONLY],
                block: [],
                always_block: [],
            },
            update: {
                always_allow: [],
                allow: [SYSTEM_ONLY],
                block: [],
                always_block: [],
            },
            delete: {
                always_allow: [],
                allow: [SYSTEM_ONLY],
                block: [],
                always_block: [],
            },
        },

        fields: [
            id: {
                r#type: FieldType::String,
                primary_key: true,
                required: true,
            },
            run_id: {
                r#type: FieldType::String,
                required: true,
            },
            record_table: {
                r#type: FieldType::String,
                required: true,
            },
            record_id: {
                r#type: FieldType::String,
                required: true,
            },
            action: {
                r#type: FieldType::Enum(&["cascade_delete", "set_null", "remove_edge"]),
                required: true,
            },
            set_null_field: {
                r#type: FieldType::String,
                required: false,
            },
            edge_table: {
                r#type: FieldType::String,
                required: false,
            },
            status: {
                r#type: FieldType::Enum(&["queued", "in_progress", "completed", "failed", "skipped"]),
                required: true,
                default: "queued",
            },
            depth: {
                r#type: FieldType::Integer,
                required: true,
            },
            connection_name: {
                r#type: FieldType::String,
                required: true,
            },
            from_table: {
                r#type: FieldType::String,
                required: true,
            },
            error_message: {
                r#type: FieldType::String,
                required: false,
            },
            started_at: {
                r#type: FieldType::DateTime,
                required: false,
            },
            completed_at: {
                r#type: FieldType::DateTime,
                required: false,
            },
        ]
    }
}
