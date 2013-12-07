package com.rethinkdb.impl;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.rethinkdb.Ql2.Term;
import com.rethinkdb.Ql2.Term.TermType;
import com.rethinkdb.RqlDriverException;
import java.lang.reflect.InvocationTargetException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public abstract class RqlQuery {

    private static final DateTimeFormatter format = DateTimeFormatter.ofPattern( "yyyy-MM-dd'T'HH:mm:ss.SSSX");
    
    protected ArrayList<RqlQuery> _args = new ArrayList<>();
    protected Map<String, Object> _optargs = new HashMap<>();

    protected RqlQuery() {
    }

    protected RqlQuery(Object... args) {
        construct( args);
    }

    private void construct(Object[] args) {
        for (Object o : args) {
            _args.add(eval(o));
        }
    }

    protected <T extends RqlQuery> T prepend_construct(Object[] args, Class<T> clazz) {
        try {
            Constructor<T> ctor = clazz.getDeclaredConstructor(Object[].class);
            Object[] o = new Object[args.length + 1];
            o[0] = this;
            System.arraycopy(args, 0, o, 1, args.length);
            return (T) ctor.newInstance(new Object[]{o});
        } 
        catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RqlDriverException( "Could not prepend arguments.", e);
        }
    }

    public RqlQuery optargs(HashMap<String, Object> args) {
        _optargs.putAll(args);
        return this;
    }

    abstract protected Term.TermType tt();

    public RqlBiOperQuery.Eq eq(Object... args) {
        return prepend_construct(args, RqlBiOperQuery.Eq.class);
    }

    public RqlBiOperQuery.Ne ne(Object... args) {
        return prepend_construct(args, RqlBiOperQuery.Ne.class);
    }

    public RqlBiOperQuery.Lt lt(Object... args) {
        return prepend_construct(args, RqlBiOperQuery.Lt.class);
    }

    public RqlBiOperQuery.Le le(Object... args) {
        return prepend_construct(args, RqlBiOperQuery.Le.class);
    }

    public RqlBiOperQuery.Gt gt(Object... args) {
        return prepend_construct(args, RqlBiOperQuery.Gt.class);
    }

    public RqlBiOperQuery.Ge ge(Object... args) {
        return prepend_construct(args, RqlBiOperQuery.Ge.class);
    }

    public RqlBiOperQuery.Add add(Object... args) {
        return prepend_construct(args, RqlBiOperQuery.Add.class);
    }

    public RqlBiOperQuery.Sub sub(Object... args) {
        return prepend_construct(args, RqlBiOperQuery.Sub.class);
    }

    public RqlBiOperQuery.Mul mul(Object... args) {
        return prepend_construct(args, RqlBiOperQuery.Mul.class);
    }

    public RqlBiOperQuery.Div div(Object... args) {
        return prepend_construct(args, RqlBiOperQuery.Div.class);
    }

    public RqlBiOperQuery.Mod mod(Object... args) {
        return prepend_construct(args, RqlBiOperQuery.Mod.class);
    }

    public RqlMethodQuery.Contains contains(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Contains.class);
    }

    public RqlMethodQuery.HasFields has_fields(Object... args) {
        return prepend_construct(args, RqlMethodQuery.HasFields.class);
    }

    public RqlMethodQuery.WithFields with_fields(Object... args) {
        return prepend_construct(args, RqlMethodQuery.WithFields.class);
    }

    public RqlMethodQuery.Keys keys(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Keys.class);
    }

    public RqlMethodQuery.Pluck pluck(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Pluck.class);
    }

    public RqlMethodQuery.Without without(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Without.class);
    }

    // Stand in for default ( default is a reserved keyword ) 
    public RqlQuery.Default def(Object... args) {
        return prepend_construct(args, RqlQuery.Default.class);
    }

    // Stand in for do ( do is a reserved keyword )
    public RqlQuery.FunCall call(Object... args) {
        return prepend_construct(args, RqlQuery.FunCall.class);
    }

    public RqlMethodQuery.Update update(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Update.class);
    }

    public RqlMethodQuery.Replace replace(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Replace.class);
    }

    public RqlMethodQuery.Delete delete(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Delete.class);
    }

    public RqlMethodQuery.CoerceTo coerce_to(Object... args) {
        return prepend_construct(args, RqlMethodQuery.CoerceTo.class);
    }

    public RqlMethodQuery.TypeOf type_of(Object... args) {
        return prepend_construct(args, RqlMethodQuery.TypeOf.class);
    }

    public RqlMethodQuery.Merge merge(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Merge.class);
    }

    public RqlMethodQuery.Append append(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Append.class);
    }

    public RqlMethodQuery.Prepend prepend(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Prepend.class);
    }

    public RqlMethodQuery.Difference difference(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Difference.class);
    }

    public RqlMethodQuery.SetInsert set_insert(Object... args) {
        return prepend_construct(args, RqlMethodQuery.SetInsert.class);
    }

    public RqlMethodQuery.SetUnion set_union(Object... args) {
        return prepend_construct(args, RqlMethodQuery.SetUnion.class);
    }

    public RqlMethodQuery.SetIntersection set_intersection(Object... args) {
        return prepend_construct(args, RqlMethodQuery.SetIntersection.class);
    }

    public RqlMethodQuery.SetDifference set_difference(Object... args) {
        return prepend_construct(args, RqlMethodQuery.SetDifference.class);
    }

    public RqlQuery.Nth nth(Object... args) {
        return prepend_construct(args, RqlQuery.Nth.class);
    }

    public RqlQuery.Match match(Object... args) {
        return prepend_construct(args, RqlQuery.Match.class);
    }

    public RqlMethodQuery.IsEmpty is_empty(Object... args) {
        return prepend_construct(args, RqlMethodQuery.IsEmpty.class);
    }

    public RqlQuery.Slice slice(Object... args) {
        return prepend_construct(args, RqlQuery.Slice.class);
    }

    public RqlMethodQuery.Skip skip(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Skip.class);
    }

    public RqlMethodQuery.Limit limit(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Limit.class);
    }

    public RqlMethodQuery.OrderBy order_by(Object... args) {
        return prepend_construct(args, RqlMethodQuery.OrderBy.class);
    }

    public RqlMethodQuery.Distinct distinct(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Distinct.class);
    }

    public RqlMethodQuery.Union union(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Union.class);
    }

    public RqlMethodQuery.InnerJoin inner_join(Object... args) {
        return prepend_construct(args, RqlMethodQuery.InnerJoin.class);
    }

    public RqlMethodQuery.OuterJoin outer_join(Object... args) {
        return prepend_construct(args, RqlMethodQuery.OuterJoin.class);
    }

    public RqlMethodQuery.Zip zip(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Zip.class);
    }

    public RqlMethodQuery.Info info(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Info.class);
    }

    public RqlMethodQuery.InsertAt insert_at(Object... args) {
        return prepend_construct(args, RqlMethodQuery.InsertAt.class);
    }

    public RqlMethodQuery.SpliceAt splice_at(Object... args) {
        return prepend_construct(args, RqlMethodQuery.SpliceAt.class);
    }

    public RqlMethodQuery.DeleteAt delete_at(Object... args) {
        return prepend_construct(args, RqlMethodQuery.DeleteAt.class);
    }

    public RqlMethodQuery.ChangeAt change_at(Object... args) {
        return prepend_construct(args, RqlMethodQuery.ChangeAt.class);
    }

    public RqlMethodQuery.Sample sample(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Sample.class);
    }

    /*
     * Note: The following are suposed to be able to use functions
     * I don't know exactly how to handle this and just want to get 
     * some of this functionality in place before I make it all perty
     */
    public RqlMethodQuery.IndexesOf indexes_of(Object... args) {
        return prepend_construct(args, RqlMethodQuery.IndexesOf.class);
    }

    public RqlMethodQuery.Reduce reduce(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Reduce.class);
    }

    public RqlMethodQuery.Map map(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Map.class);
    }

    public RqlMethodQuery.Filter filter(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Filter.class);
    }

    public RqlMethodQuery.ConcatMap concat_map(Object... args) {
        return prepend_construct(args, RqlMethodQuery.ConcatMap.class);
    }

    public RqlMethodQuery.Between between(Object... args) {
        return prepend_construct(args, RqlMethodQuery.Between.class);
    }

    public RqlMethodQuery.EqJoin eq_join(Object... args) {
        return prepend_construct(args, RqlMethodQuery.EqJoin.class);
    }

    public RqlMethodQuery.GroupedMapReduce grouped_map_reduce(Object... args) {
        return prepend_construct(args, RqlMethodQuery.GroupedMapReduce.class);
    }

    public RqlMethodQuery.GroupBy group_by(Object... args) {
        return prepend_construct(args, RqlMethodQuery.GroupBy.class);
    }

    public RqlMethodQuery.ForEach for_each(Object... args) {
        return prepend_construct(args, RqlMethodQuery.ForEach.class);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> RqlQuery eval(T t) {
        if (t instanceof RqlQuery) {
            return (RqlQuery) t;
        }
        if (t instanceof List) {
            return new MakeArray((List) t);
        }
        if (t instanceof Map) {
            return new MakeObj((Map) t);
        }
        if( t instanceof LocalDateTime) {
            return new Iso8601( format.format( (LocalDateTime)t));
        }
        return new RqlQuery.Datum(t);
    }
    
    public Term build() {
        Term.Builder t = Term.newBuilder().setType(tt());
        
        for (RqlQuery q : _args) {
            t.addArgs(q.build());
        }

        for (Entry<String, Object> e : _optargs.entrySet()) {
            t.addOptargs( Term.AssocPair.newBuilder()
                            .setKey(e.getKey())
                            .setVal(eval(e.getValue()).build())
                            .build()
            );
        }
        
        return t.build();
    }

    public static class Datum extends RqlQuery {

        private final Object _data;

        public <T> Datum(T t) {
            super();
            _data = t;
        }

        @Override
        protected TermType tt() {
            return Term.TermType.DATUM;
        }

        @Override
        public Term build() {
            return Term.newBuilder()
                    .setType(tt())
                    .setDatum(com.rethinkdb.impl.Datum.datum(_data))
                    .build();
        }
    }

    public static class MakeArray extends RqlQuery {

        public <T> MakeArray(List<T> l) {
            super();
            
            for (T t : l) {
                _args.add(eval(t));
            }
        }

        @Override
        protected TermType tt() {
            return Term.TermType.MAKE_ARRAY;
        }
    }

    public static class MakeObj extends RqlQuery {

        public <V extends RqlQuery> MakeObj(Map<String, V> m) {
            super();
            _optargs.putAll(m);
        }

        @Override
        protected TermType tt() {
            return Term.TermType.MAKE_OBJ;
        }
    }

    public static class Var extends RqlQuery {

        public Var(Object... args) {
            super(args);
        }

        @Override
        protected TermType tt() {
            return Term.TermType.VAR;
        }
    }

    public static class Default extends RqlQuery {

        public Default(Object... args) {
            super(args);
        }

        @Override
        protected TermType tt() {
            return Term.TermType.DEFAULT;
        }
    }

    public static class ImplicitVar extends RqlQuery {

        public ImplicitVar(Object... args) {
            super(args);
        }

        @Override
        protected TermType tt() {
            return Term.TermType.IMPLICIT_VAR;
        }
    }

    public static class Not extends RqlQuery {

        public Not(Object... args) {
            super(args);
        }

        @Override
        protected TermType tt() {
            return Term.TermType.NOT;
        }
    }

    public static class Slice extends RqlQuery {

        public Slice(Object... args) {
            super(args);
        }

        @Override
        protected TermType tt() {
            return Term.TermType.SLICE;
        }
    }

    public static class GetField extends RqlQuery {

        public GetField(Object... args) {
            super(args);
        }

        @Override
        protected TermType tt() {
            return Term.TermType.GET_FIELD;
        }
    }

    public static class FunCall extends RqlQuery {

        public FunCall(Object... args) {
            super(args);
        }

        @Override
        protected TermType tt() {
            return Term.TermType.FUNCALL;
        }
    }
    
    public static class Iso8601 extends RqlQuery {
        public Iso8601( Object... args) {
            super( args);
        }
        
        @Override
        protected TermType tt() {
            return Term.TermType.ISO8601;
        }
    }

    public static class Table extends RqlQuery {

        public Table(Object... args) {
            super(args);
        }

        @Override
        protected TermType tt() {
            return Term.TermType.TABLE;
        }

        public RqlMethodQuery.Insert insert(Object... args) {
            return prepend_construct(args, RqlMethodQuery.Insert.class);
        }

        public RqlMethodQuery.Get get(Object... args) {
            return prepend_construct(args, RqlMethodQuery.Get.class);
        }

        public RqlMethodQuery.GetAll get_all(Object... args) {
            return prepend_construct(args, RqlMethodQuery.GetAll.class);
        }

        public RqlMethodQuery.IndexCreate index_create(Object... args) {
            return prepend_construct(args, RqlMethodQuery.IndexCreate.class);
        }

        public RqlMethodQuery.IndexDrop index_drop(Object... args) {
            return prepend_construct(args, RqlMethodQuery.IndexDrop.class);
        }

        public RqlMethodQuery.IndexList index_list(Object... args) {
            return prepend_construct(args, RqlMethodQuery.IndexList.class);
        }

        @Override
        public RqlMethodQuery.Filter filter(Object... args) {
            return prepend_construct(args, RqlMethodQuery.Filter.class);
        }
    }

    public static class Nth extends RqlQuery {

        public Nth(Object... args) {
            super(args);
        }

        @Override
        protected TermType tt() {
            return Term.TermType.NTH;
        }
    }

    public static class Match extends RqlQuery {

        public Match(Object... args) {
            super(args);
        }

        @Override
        protected TermType tt() {
            return Term.TermType.MATCH;
        }
    }

    public static class Func extends RqlQuery {

        public Func(Object... args) {
            super(args);
        }

        @Override
        protected TermType tt() {
            return Term.TermType.FUNC;
        }
    }
}
