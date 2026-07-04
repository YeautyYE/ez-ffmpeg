//! Pure-Rust subtitle rendering: fonts, shaping, rasterization, layout.
//!
//! Implements [`crate::subtitle::backend::SubtitleRenderer`] on top of the
//! [`crate::subtitle::ass`] parser without linking libass. The module split
//! mirrors the pipeline: `fonts` (discovery + matching) -> `shape` (bidi +
//! rustybuzz) -> `raster` (zeno fill/stroke + blur) -> `layout` (styling
//! state machine, wrapping, alignment) -> `renderer` (the trait impl and
//! frame-time entry point).

pub(crate) mod fonts;
pub(crate) mod layout;
pub(crate) mod raster;
pub(crate) mod renderer;
pub(crate) mod shape;

pub(crate) use renderer::PureRenderer;
