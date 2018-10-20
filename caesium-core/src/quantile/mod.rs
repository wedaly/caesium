pub mod baseline;
mod compactor;
pub mod error;
pub mod kll;
mod minmax;
pub mod readable;

pub mod writable {
    #[cfg(not(feature = "baseline"))]
    pub use quantile::kll::KllSketch as WritableSketch;

    #[cfg(feature = "baseline")]
    pub use quantile::baseline::BaselineSketch as WritableSketch;
}

#[cfg(test)]
mod tests;
