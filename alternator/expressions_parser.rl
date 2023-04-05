#pragma once

#include <vector>
#include <string_view>

#include "alternator/expressions_types.hh"
#include "alternator/expressions.hh"

#include "alternator/expressions_base.hh"

/*
 * The DynamoDB protocol is based on JSON, and most DynamoDB requests
 * describe the operation and its parameters via JSON objects such as maps
 * and lists. Nevertheless, in some types of requests an "expression" is
 * passed as a single string, and we need to parse this string. These
 * cases include:
 *  1. Attribute paths, such as "a[3].b.c", are used in projection
 *     expressions as well as inside other expressions described below.
 *  2. Condition expressions, such as "(NOT (a=b OR c=d)) AND e=f",
 *     used in conditional updates, filters, and other places.
 *  3. Update expressions, such as "SET #a.b = :x, c = :y DELETE d"
 *
 * All these expression syntaxes are very simple: Most of them could be
 * parsed as regular expressions, and the parenthesized condition expression
 * could be done with a simple hand-written lexical analyzer and recursive-
 * descent parser. Nevertheless, we decided to specify these parsers in the
 * Ragel language already used in the Scylla project, hopefully making these
 * parsers easier to reason about, and easier to change if needed - and
 * reducing the amount of boiler-plate code.
 */

%%{
machine projection_parser;
include base "expressions_base.rl";
access _fsm_;

# embeding spaces directly inside path would have bad effect on matching
# other states which embed path but not consider space as the beginning of path matching
spaced_path = space* path space* ;
main := spaced_path (',' spaced_path)* ;
}%%

%%{
machine update_parser;
include base "expressions_base.rl";
access _fsm_;

set_rhs = value (space* ('+'|'-') space* value)* ;
set_action = space* path space* '=' space* set_rhs space* ;
remove_action = space* path space* ;
add_action = space* path space valref space* ;
delete_action = space* path space valref space* ;

expression_clause =
      (/SET/i set_action (',' set_action)*)
    | (/ADD/i add_action (',' add_action)*)
    | (/REMOVE/i remove_action (',' remove_action)*)
    | (/DELETE/i delete_action (',' delete_action)*)
;

update_expression = expression_clause (space expression_clause)* ;
main := update_expression ;
}%%

namespace alternator {

class parser_base {
protected:
    int _fsm_cs; // Ragel's internal thing.

    // Used only by parsers with recursion (e.g with fcall/fret)
    // but since it's part of base machine we need it everywhere.
    static constexpr int _nesting_limit = 32;
    int _fsm_stack[_nesting_limit];
    int _fsm_top;

    // Pointers to start and end of current "token" match
    // it's flexibly used in FSM by calls to mark_start and mark_end.
    const char* _cur_start;
    const char* _cur_end;

    [[gnu::always_inline]]
    void mark_start(const char* p) {
        _cur_start = p;
    }

    [[gnu::always_inline]]
    void mark_end(const char* p) {
        _cur_end = p;
    }

    [[gnu::always_inline]]
    std::string str() {
        // We do copy here to simplify memory management outside the parser
        // but if input's string_view data is guarnteed to outlive any usage of
        // parse result we could use string_view here.
        return std::string(_cur_start, _cur_end - _cur_start);
    }

    // Those names are hardcoded in generated code, they serve as an 'interface' for Ragel.
    const char* p;
    const char* pe;
    const char* eof;

    void init(std::string_view& buf) {
        p = buf.data();
        pe = p + buf.size();
        eof = pe;
        _cur_start = _cur_end = p;
    }

    void throw_on_error(int first_final_state) {
        if (_fsm_cs < first_final_state) {
            throw expressions_syntax_error(format("Parse error after position {}", eof - _cur_start));
        }
        if (p < pe) {
            // ended in final state but some data left to read
            throw expressions_syntax_error(format("Parse error after position {}", eof - _cur_start));
        }
    }

    // Ragel requires all action functions to be defined even if not "used" (no transition in FSM
    // which would trigger it). So we put empty implementation and overload if derived parser needs it.

    void observe_path() {}
    void observe_path_root() {}
    void observe_path_dot() {}
    void observe_path_index() {}

    void observe_value_valref() {}
    void observe_value_path() {}
    void observe_value_func_name() {}
    void observe_value_func_param() {}
    void observe_value_start() {}
};

class parser_path_handler : private virtual parser_base {
protected:
    parsed::path _cur_path;

    [[gnu::always_inline]]
    void observe_path_root() {
        _cur_path.set_root(str());
    }

    [[gnu::always_inline]]
    void observe_path_dot() {
        _cur_path.add_dot(str());
    }

    [[gnu::always_inline]]
    void observe_path_index() {
       _cur_path.add_index(std::stoi(str()));
    }
};

class parser_value_handler : 
    private virtual parser_base,
    private virtual parser_path_handler {
protected:
    std::vector<parsed::value> _value_stack;
    parsed::value* _cur_value; // Points to top element from the stack.

    [[gnu::always_inline]]
    void observe_value_start() {
        _cur_value = &_value_stack.emplace_back();
    }

    [[gnu::always_inline]]
    void observe_value_path() {
        _cur_value->set_path(std::move(_cur_path));
        _cur_path = parsed::path();
    }

    [[gnu::always_inline]]
    void observe_value_valref() {
        _cur_value->set_valref(str());
    }

    [[gnu::always_inline]]
    void observe_value_func_name() {
        _cur_value->set_func_name(str());
    }

    [[gnu::always_inline]]
    void observe_value_func_param() {
        auto si = _value_stack.size();
        assert(si >= 2);
        _cur_value = &_value_stack[si - 2];
        _cur_value->add_func_parameter(std::move(_value_stack.back()));
        _value_stack.pop_back();
    }
};

class projection_parser : 
    private virtual parser_base,
    private virtual parser_path_handler 
{
    %% machine projection_parser;
    %% write data noerror nofinal noprefix;
    std::vector<parsed::path> _res;
public:
    std::vector<parsed::path> parse(std::string_view buf) {
        %% write init;
        parser_base::init(buf);
        _res.clear();
        %% write exec;
        throw_on_error(%%{ write first_final; }%%);
        return _res;
    }
private:
    [[gnu::always_inline]]
    void observe_path() {
        _res.push_back(std::move(_cur_path));
        _cur_path = parsed::path();
    }
};

class update_parser : 
private virtual parser_base,
    private virtual parser_path_handler,
    private virtual parser_value_handler 
{
    %% machine projection_parser;
    %% write data noerror nofinal noprefix;
    parsed::update_expression _res;
public:
    parsed::update_expression parse(std::string_view buf) {
        %% write init;
        init(buf);
        _res = parsed::update_expression();
        %% write exec;
        throw_on_error(%%{ write first_final; }%%);
        return _res;
    }
private:
    // [[gnu::always_inline]]
    // void observe_path() {
    //     _res.push_back(std::move(_cur_path));
    //     _cur_path = parsed::path();
    // }
};

} // namespace alternator