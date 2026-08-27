// ValenceIterRun — included from crate root (`mod valence_iter_run_schema` in lib.rs);
// Valence schema macros + build-time codegen register the model.
// Field is `target_table` (not `table_name`) to avoid colliding with generated `table_name()` getters vs `Model::table_name()`.

use ::valence::prelude::*;
use ::valence::privacy_policies::common::SYSTEM_ONLY;


valence_schema! {
    ValenceIterRun {
        table: "valence_iter_run",
        version: "0.1.0",
        database: crate::DEFAULT_PLATFORM_STORAGE,
        description: "Tracks a single execution of a ValenceIter across a table",

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
            iter_name: {
                r#type: FieldType::String,
                required: true,
            },
            target_table: {
                r#type: FieldType::String,
                required: true,
            },
            status: {
                r#type: FieldType::Enum(&["pending", "scanning", "processing", "completed", "failed", "cancelled"]),
                required: true,
            },
            total_rows: {
                r#type: FieldType::Integer,
                required: true,
                default: 0,
            },
            scanned_rows: {
                r#type: FieldType::Integer,
                required: true,
                default: 0,
            },
            processed_rows: {
                r#type: FieldType::Integer,
                required: true,
                default: 0,
            },
            skipped_rows: {
                r#type: FieldType::Integer,
                required: true,
                default: 0,
            },
            failed_rows: {
                r#type: FieldType::Integer,
                required: true,
                default: 0,
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
            created_at: {
                r#type: FieldType::DateTime,
                required: true,
            },
            initiated_by: {
                r#type: FieldType::String,
                required: true,
            },
            target_row_id: {
                r#type: FieldType::String,
                required: false,
            },
        ]
    }
}
