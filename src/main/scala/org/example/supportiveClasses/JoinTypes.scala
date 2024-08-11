package org.example.supportiveClasses

case class JoinTypes(
                     inner: String = "inner",
                     outer: String = "outer",
                     leftOuter: String = "left_outer",
                     rightOuter: String = "right_outer",
                     leftSemi: String = "left_semi",
                     leftAnti: String = "left_anti",
                     crossJoin: String = "cross"
                     )
