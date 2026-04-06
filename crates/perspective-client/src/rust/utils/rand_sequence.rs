// ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
// ┃ ██████ ██████ ██████       █      █      █      █      █ █▄  ▀███ █       ┃
// ┃ ▄▄▄▄▄█ █▄▄▄▄▄ ▄▄▄▄▄█  ▀▀▀▀▀█▀▀▀▀▀ █ ▀▀▀▀▀█ ████████▌▐███ ███▄  ▀█ █ ▀▀▀▀▀ ┃
// ┃ █▀▀▀▀▀ █▀▀▀▀▀ █▀██▀▀ ▄▄▄▄▄ █ ▄▄▄▄▄█ ▄▄▄▄▄█ ████████▌▐███ █████▄   █ ▄▄▄▄▄ ┃
// ┃ █      ██████ █  ▀█▄       █ ██████      █      ███▌▐███ ███████▄ █       ┃
// ┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
// ┃ Copyright (c) 2017, the Perspective Authors.                              ┃
// ┃ ╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌ ┃
// ┃ This file is part of the Perspective library, distributed under the terms ┃
// ┃ of the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). ┃
// ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

//! This implementation liberally borrows from the excellent
//! [`rand-unique`](https://github.com/hoxxep/rand-unique) crate, MIT licensed.
//! Unlike `rand-unique`, this hacked up version only implements u32 (for wasm),
//! but is compatible with `rand>0.8` and ~20kb smaller (in wasm).

const INIT_BASE: u32 = 0x682f0161;
const INIT_OFFSET: u32 = 0x46790905;
const PRIME: u32 = 4294967291;
const INTERMEDIATE_XOR: u32 = 0x5bf03635;

#[derive(Debug, Clone)]
pub struct RandomSequence {
    start_index: u32,
    current_index: u32,
    intermediate_offset: u32,
    ended: bool,
}

impl RandomSequence {
    pub fn new(seed_base: u32, seed_offset: u32) -> Self {
        let start_index = permute_qpr(permute_qpr(seed_base).wrapping_add(INIT_BASE));
        let intermediate_offset = permute_qpr(permute_qpr(seed_offset).wrapping_add(INIT_OFFSET));
        RandomSequence {
            start_index,
            current_index: 0,
            intermediate_offset,
            ended: false,
        }
    }
}

impl Iterator for RandomSequence {
    type Item = u32;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let index = self.start_index.wrapping_add(self.current_index);
        let inner_residue = permute_qpr(index).wrapping_add(self.intermediate_offset);
        let next = permute_qpr(inner_residue ^ INTERMEDIATE_XOR);
        self.current_index = match self.current_index.checked_add(1) {
            Some(v) => {
                self.ended = false;
                v
            },
            None => {
                if !self.ended {
                    self.ended = true;
                    self.current_index
                } else {
                    return None;
                }
            },
        };

        Some(next)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (u32::MAX as usize - self.current_index as usize, None)
    }
}

fn permute_qpr(x: u32) -> u32 {
    if x >= PRIME {
        return x;
    }

    let residue = ((x as u64 * x as u64) % PRIME as u64) as u32;
    if x <= PRIME >> 1 {
        residue
    } else {
        PRIME - residue
    }
}
