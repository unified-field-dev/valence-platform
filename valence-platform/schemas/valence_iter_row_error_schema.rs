// ValenceIterRowError — included from crate root (`mod valence_iter_row_error_schema` in lib.rs).

use ::valence::prelude::*;
use ::valence::privacy_policies::common::SYSTEM_ONLY;


valence_schema! {
    ValenceIterRowError {
        table: "valence_iter_row_error",
        version: "0.1.0",
        database: crate::DEFAULT_PLATFORM_STORAGE,
        description: "Error record for a single row that failed during iter execution",

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
            batch_id: {
                r#type: FieldType::String,
                required: false,
            },
            row_id: {
                r#type: FieldType::String,
                required: true,
            },
            error_message: {
                r#type: FieldType::String,
                required: true,
            },
            error_kind: {
                r#type: FieldType::Enum(&["should_run_error", "execute_error"]),
                required: true,
            },
            created_at: {
                r#type: FieldType::DateTime,
                required: true,
            },
        ]
    }
}
