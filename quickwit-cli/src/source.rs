// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::rc::Rc;

use anyhow::{bail, Context};
use clap::ArgMatches;
use itertools::Itertools;
use quickwit_common::uri::normalize_uri;
use quickwit_metastore::MetastoreUriResolver;
use serde_json::Value;
use tabled::{Alignment, Header, Modify, Row, Style, Table, Tabled};

#[derive(Debug, PartialEq)]
pub struct DescribeSourceArgs {
    pub metastore_uri: String,
    pub index_id: String,
    pub source_id: String,
}

#[derive(Debug, PartialEq)]
pub struct ListSourcesArgs {
    pub metastore_uri: String,
    pub index_id: String,
}

#[derive(Debug, PartialEq)]
pub enum SourceCliCommand {
    DescribeSource(DescribeSourceArgs),
    ListSources(ListSourcesArgs),
}

impl SourceCliCommand {
    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::DescribeSource(args) => describe_source_cli(args).await,
            Self::ListSources(args) => list_sources_cli(args).await,
        }
    }

    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .subcommand()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse sub-matches."))?;
        let parsed_subcommand = match subcommand {
            "describe" => Self::DescribeSource(Self::parse_describe_args(submatches)?),
            "list" => Self::ListSources(Self::parse_list_args(submatches)?),
            _ => bail!("Source subcommand `{}` is not implemented.", subcommand),
        };
        Ok(parsed_subcommand)
    }

    fn parse_describe_args(matches: &ArgMatches) -> anyhow::Result<DescribeSourceArgs> {
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(normalize_uri)
            .expect("`metastore-uri` is a required arg.")?;
        let index_id = matches
            .value_of("index-id")
            .map(String::from)
            .expect("`index-id` is a required arg.");
        let source_id = matches
            .value_of("source-id")
            .map(String::from)
            .expect("`source-id` is a required arg.");
        Ok(DescribeSourceArgs {
            metastore_uri,
            index_id,
            source_id,
        })
    }

    fn parse_list_args(matches: &ArgMatches) -> anyhow::Result<ListSourcesArgs> {
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(normalize_uri)
            .expect("`metastore-uri` is a required arg.")?;
        let index_id = matches
            .value_of("index-id")
            .map(String::from)
            .expect("`index-id` is a required arg.");
        Ok(ListSourcesArgs {
            metastore_uri,
            index_id,
        })
    }
}

#[derive(Tabled)]
struct CheckpointRow {
    #[header("Partition ID")]
    partition_id: String,
    #[header("Offset")]
    offset: String,
}

#[derive(Tabled)]
struct ParamsRow {
    #[header("Key")]
    key: Rc<String>,
    #[header("Value")]
    value: Value,
}

async fn describe_source_cli(args: DescribeSourceArgs) -> anyhow::Result<()> {
    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;
    let index_metadata = metastore.index_metadata(&args.index_id).await?;

    let source = index_metadata
        .sources
        .into_iter()
        .find(|source| source.source_id == args.source_id)
        .with_context(|| format!("Source `{}` does not exist.", args.source_id))?;

    let source_rows = vec![SourceRow {
        source_id: source.source_id,
        source_type: source.source_type,
    }];
    let source_table = make_table(source_rows, "Source");

    let params_rows = flatten_json(source.params)
        .into_iter()
        .map(|(key, value)| ParamsRow {
            key,
            value,
        })
        .sorted_by(|left, right| left.key.cmp(&right.key));
    let params_table = make_table(params_rows, "Parameters");

    let checkpoint_rows = index_metadata
        .checkpoint
        .iter()
        .map(|(partition_id, position)| CheckpointRow {
            partition_id: partition_id.0.to_string(),
            offset: position.as_str().to_string(),
        })
        .sorted_by(|left, right| left.partition_id.cmp(&right.partition_id));
    let checkpoint_table = make_table(checkpoint_rows, "Checkpoint");

    println!("{}\n\n{}\n\n{}", source_table, params_table, checkpoint_table);
    Ok(())
}

#[derive(Tabled)]
struct SourceRow {
    #[header("ID")]
    source_id: String,
    #[header("Type")]
    source_type: String,
}

async fn list_sources_cli(args: ListSourcesArgs) -> anyhow::Result<()> {
    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;
    let index_metadata = metastore.index_metadata(&args.index_id).await?;
    let rows = index_metadata
        .sources
        .into_iter()
        .map(|source| SourceRow {
            source_id: source.source_id,
            source_type: source.source_type,
        })
        .sorted_by(|left, right| left.source_id.cmp(&right.source_id));
    let table = make_table(rows, "Sources");
    println!("{}", table);
    Ok(())
}

fn flatten_json(value: Value) -> Vec<(Rc<String>, Value)> {
    let mut acc = Vec::new();
    let mut values = vec![(Rc::new(String::new()), value)];

    while let Some((root, value)) = values.pop() {
        if let Value::Object(obj) = value {
            for (key, val) in obj {
                values.push((
                    if root.is_empty() {
                        Rc::new(key)
                    } else {
                        Rc::new(format!("{}.{}", root, key))
                    },
                    val,
                ));
            }
            continue;
        }
        acc.push((root.clone(), value))
    }
    acc
}

fn make_table<T: Tabled>(rows: impl IntoIterator<Item = T>, header: &str) -> Table {
    Table::new(rows)
        .with(Header(header))
        .with(Modify::new(Row(2..)).with(Alignment::left()))
        .with(Style::psql())
}
