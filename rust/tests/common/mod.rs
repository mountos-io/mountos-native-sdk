//! Wire messages are non_exhaustive, so a test cannot build one by struct literal. `build!` takes
//! the literal's field list and sets each field on a default message.

#![allow(unused_macros)]

macro_rules! build {
  ($t:path { $($body:tt)* }) => {{
    let mut m = <$t>::default();
    build!(@set m; $($body)*);
    m
  }};
  (from $b:expr; $($body:tt)*) => {{
    let mut m = $b;
    build!(@set m; $($body)*);
    m
  }};
  (@set $m:ident;) => {};
  (@set $m:ident; $f:ident : $v:expr, $($rest:tt)*) => {
    $m.$f = $v;
    build!(@set $m; $($rest)*);
  };
  (@set $m:ident; $f:ident : $v:expr) => {
    $m.$f = $v;
  };
  (@set $m:ident; $f:ident, $($rest:tt)*) => {
    $m.$f = $f;
    build!(@set $m; $($rest)*);
  };
  (@set $m:ident; $f:ident) => {
    $m.$f = $f;
  };
}
