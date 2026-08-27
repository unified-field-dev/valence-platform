// ValenceIterBatch — included from crate root (`mod valence_iter_batch_schema` in lib.rs).

use ::valence::prelude::*;
use ::valence::privacy_policies::common::SYSTEM_ONLY;


valence_schema! {
    ValenceIterBatch {
        table: "valence_iter_batch",
        version: "0.1.0",
        database: crate::DEFAULT_PLATFORM_STORAGE,
        description: "One page/batch of rows within an iter run",

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
            batch_index: {
                r#type: FieldType::Integer,
                required: true,
            },
            status: {
                r#type: FieldType::Enum(&["pending", "enqueuing", "processing", "completed", "failed"]),
                required: true,
            },
            row_count: {
                r#type: FieldType::Integer,
                required: true,
            },
            enqueued_count: {
                r#type: FieldType::Integer,
                required: true,
                default: 0,
            },
            processed: {
                r#type: FieldType::Integer,
                required: true,
                default: 0,
            },
            skipped: {
                r#type: FieldType::Integer,
                required: true,
                default: 0,
            },
            failed: {
                r#type: FieldType::Integer,
                required: true,
                default: 0,
            },
            cursor: {
                r#type: FieldType::String,
                required: false,
            },
            created_at: {
                r#type: FieldType::DateTime,
                required: true,
            },
            completed_at: {
                r#type: FieldType::DateTime,
                required: false,
            },
        ]
    }
}
