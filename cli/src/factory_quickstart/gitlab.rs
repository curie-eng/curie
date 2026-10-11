//! The GitLab code host and tracker for `curie factory quickstart` (#4397). Reserved: this
//! build has no adapter for it, so a quickstart naming `gitlab` stops here.

use anyhow::Result;

use super::{forge_not_installed, ForgeFlags};

pub(super) fn check(_forge: &ForgeFlags) -> Result<()> {
    Err(forge_not_installed("gitlab"))
}
