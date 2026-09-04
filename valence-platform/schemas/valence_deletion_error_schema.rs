// ValenceDeletionError — per-step failure row for a deletion run.

use ::valence::prelude::*;
use ::valence::privacy_policies::common::SYSTEM_ONLY;


valence_schema! {
    ValenceDeletionError {
        table: "valence_deletion_error",
        version: "0.1.0",
        database: crate::DEFAULT_PLATFORM_STORAGE,
        description: "Error record for a single deletion step that failed",

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
            step_id: {
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
            error_message: {
                r#type: FieldType::String,
                required: true,
            },
            created_at: {
                r#type: FieldType::DateTime,
                required: true,
            },
        ]
    }
}
