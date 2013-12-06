package com.rethinkdb.impl;

import com.rethinkdb.Ql2.Term.TermType;
import com.rethinkdb.Ql2.Term;

public abstract class RqlBiOperQuery extends RqlQuery {

    protected RqlBiOperQuery( Object... args) {
        super( args);
    }
    
    public static class Eq extends RqlBiOperQuery {

        public Eq(Object... args) {
            super(args);
        }

        @Override
        public TermType tt() {
            return Term.TermType.EQ;
        }
    }

    public static class Ne extends RqlBiOperQuery {

        public Ne(Object... args) {
            super(args);
        }

        @Override
        public TermType tt() {
            return Term.TermType.NE;
        }
    }

    public static class Lt extends RqlBiOperQuery {

        public Lt(Object... args) {
            super(args);
        }

        @Override
        public TermType tt() {
            return Term.TermType.LT;
        }
    }

    public static class Le extends RqlBiOperQuery {

        public Le(Object... args) {
            super(args);
        }

        @Override
        public TermType tt() {
            return Term.TermType.LE;
        }
    }

    public static class Gt extends RqlBiOperQuery {

        public Gt(Object... args) {
            super(args);
        }

        @Override
        public TermType tt() {
            return Term.TermType.GT;
        }
    }

    public static class Ge extends RqlBiOperQuery {

        public Ge(Object... args) {
            super(args);
        }

        @Override
        public TermType tt() {
            return Term.TermType.GE;
        }
    }

    public static class Add extends RqlBiOperQuery {

        public Add(Object... args) {
            super(args);
        }

        @Override
        public TermType tt() {
            return Term.TermType.ADD;
        }
    }

    public static class Sub extends RqlBiOperQuery {

        public Sub(Object... args) {
            super(args);
        }

        @Override
        public TermType tt() {
            return Term.TermType.SUB;
        }
    }

    public static class Mul extends RqlBiOperQuery {

        public Mul(Object... args) {
            super(args);
        }

        @Override
        public TermType tt() {
            return Term.TermType.MUL;
        }
    }

    public static class Div extends RqlBiOperQuery {

        public Div(Object... args) {
            super(args);
        }

        @Override
        public TermType tt() {
            return Term.TermType.DIV;
        }
    }

    public static class Mod extends RqlBiOperQuery {

        public Mod(Object... args) {
            super(args);
        }

        @Override
        public TermType tt() {
            return Term.TermType.MOD;
        }
    }

    public static class Any extends RqlBiOperQuery {

        public Any(Object... args) {
            super(args);
        }

        @Override
        public TermType tt() {
            return Term.TermType.ANY;
        }
    }

    public static class All extends RqlBiOperQuery {

        public All(Object... args) {
            super(args);
        }

        @Override
        public TermType tt() {
            return Term.TermType.ALL;
        }
    }
}
