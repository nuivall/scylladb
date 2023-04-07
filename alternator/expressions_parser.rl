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

namespace alternator {

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

action observe_set_rhs_value { observe_set_rhs_value(); }
action observe_set_rhs_plus_value { observe_set_rhs_plus_value(); }
action observe_set_rhs_minus_value { observe_set_rhs_minus_value(); }
action observe_set_action { observe_set_action(); }
action observe_set_action_path { observe_set_action_path(); }
action observe_remove_action { observe_remove_action(); }
action observe_add_action { observe_add_action(); }
action observe_delete_action { observe_delete_action(); }

set_rhs = value %observe_set_rhs_value space*
    (
          '+' space* value %observe_set_rhs_plus_value
        | '-' space* value %observe_set_rhs_minus_value
    )? ;
set_action = space* path %observe_set_action_path space*
    '=' space* set_rhs %observe_set_action space* ;
remove_action = space* path %observe_remove_action space* ;
add_action = space* (path space valref) %observe_add_action space* ;
delete_action = space* (path space valref) %observe_delete_action space* ;

expression_clause =
      (/SET/i set_action (',' set_action)*)
    | (/ADD/i add_action (',' add_action)*)
    | (/REMOVE/i remove_action (',' remove_action)*)
    | (/DELETE/i delete_action (',' delete_action)*)
;

update_expression = expression_clause (space expression_clause)* ;
main := update_expression ;
}%%

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
            std::cout << "throw_on_error:" << _fsm_cs << " ffin " << first_final_state << std::endl;
            throw expressions_syntax_error(format("Parse error after position {}", eof - _cur_start));
        }
        if (p < pe) {
            // ended in final state but some data left to read
            std::cout << "throw_on_error(p<pe):" << p << " < " << pe << std::endl;
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
    parsed::path _cur_path;
protected:
    [[gnu::always_inline]]
    void observe_path_root() {
        std::cout << "observe_path_root" << std::endl;
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

    [[gnu::always_inline]]
    parsed::path move_cur_path() {
        std::cout << "move_cur_path" << std::endl;
        return std::exchange(_cur_path, parsed::path());
    }
};

class parser_value_handler : 
    private virtual parser_base,
    private virtual parser_path_handler {

    std::vector<parsed::value> _value_stack;
    parsed::value* _cur_value; // Points to top element from the stack.
protected:
    [[gnu::always_inline]]
    void observe_value_start() {
        _cur_value = &_value_stack.emplace_back();
    }

    [[gnu::always_inline]]
    void observe_value_path() {
        _cur_value->set_path(move_cur_path());
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

    [[gnu::always_inline]]
    parsed::value move_cur_value() {
        assert(_value_stack.size() == 1);
        auto v = std::move(_value_stack[0]);
        _value_stack.clear();
        return v;
    }
};

class projection_parser : 
    private virtual parser_base,
    private virtual parser_path_handler 
{
    %% machine projection_parser;
    %% write data;
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
        _res.push_back(move_cur_path());
    }
};

class update_parser :
private virtual parser_base,
    private virtual parser_path_handler,
    private virtual parser_value_handler
{
    %% machine update_parser;
    %% write data;
    parsed::update_expression _res;
    // Used only for SET action, other can be constructed directly in _res.
    parsed::action _cur_action; 
    parsed::set_rhs _cur_rhs;
public:
    parsed::update_expression parse(std::string_view buf) {
        %% write init;
        init(buf);
        _res.clear();
        _res = parsed::update_expression();
        %% write exec;
        //std::cout << "!!!" <<  update_parser_name_error << std::endl;
        throw_on_error(%%{ write first_final; }%%);
        return _res;
    }
private:
    [[gnu::always_inline]]
    void observe_set_rhs_value() {
        _cur_rhs.set_value(move_cur_value());
    }

    [[gnu::always_inline]]
    void observe_set_rhs_plus_value() {
        _cur_rhs.set_plus(move_cur_value());
    }

    [[gnu::always_inline]]
    void observe_set_rhs_minus_value() {
        _cur_rhs.set_minus(move_cur_value());
    }

    [[gnu::always_inline]]
    void observe_set_action_path() {
       _cur_action.assign_set(move_cur_path());
    }
    
    [[gnu::always_inline]]
    parsed::action move_cur_action() {
        return std::exchange(_cur_action, parsed::action());
    }

    [[gnu::always_inline]]
    parsed::set_rhs move_cur_rhs() {
        return std::exchange(_cur_rhs, parsed::set_rhs());
    }

    [[gnu::always_inline]]
    void observe_set_action() {
        auto a = move_cur_action();
        a.assign_set_rhs(move_cur_rhs());
        _res.add(std::move(a));
    }
    
    [[gnu::always_inline]]
    void observe_remove_action() {
        std::cout << "observe_remove_action" << std::endl;
        parsed::action a;
        a.assign_remove(move_cur_path());
        _res.add(std::move(a));
    }
    
    [[gnu::always_inline]]
    void observe_add_action() {
        parsed::action a;
        a.assign_add(move_cur_path(), str());
        _res.add(std::move(a));
    }
    
    [[gnu::always_inline]]
    void observe_delete_action() {
        parsed::action a;
        a.assign_del(move_cur_path(), str());
        _res.add(std::move(a));
    }
};

} // namespace alternator