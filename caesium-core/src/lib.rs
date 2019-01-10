extern crate byteorder;
extern crate rand;
extern crate slab;

#[macro_use]
pub mod encode;
pub mod protocol;
pub mod quantile;
pub mod time;

#[derive(Debug)]
pub enum SketchType {
    Baseline,
    KllNoSampler,
    KllWithSampler,
}

pub fn get_sketch_type() -> SketchType {
    if cfg!(feature = "baseline") {
        SketchType::Baseline
    } else if cfg!(feature = "nosampler") {
        SketchType::KllNoSampler
    } else {
        SketchType::KllWithSampler
    }
}
