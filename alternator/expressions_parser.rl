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

/*
RAGEL DEBUGGING TIPS:
- add action ${std::cout << "! state " << fcurs << " char " << fc << " next_state " << ftargs << std::endl;} to main block, it will print every state transition
- generate FSM graph with "ragel -Vp -S "update_parser" alternator/expressions_parser.rl -o out.dot && dot out.dot -Tsvg -o out.svg"
- now you can observe and understand how FSM advances
*/

namespace alternator {

%%{
machine projection_parser;
include base "expressions_base.rl";
access _fsm_;

# Embeding spaces directly inside path would have bad effect on matching
# other states which embed path but not consider space as the beginning of path matching.
spaced_path = space* path space*;
main := spaced_path (',' spaced_path)*;
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
# This construction differentiates between one clause with multiple actions
# and multiple same clause types (e.g. "REMOVE a, b" versus "REMOVE a REMOVE b"), the latter is forbidden.
action observe_expression_clause { observe_expression_clause(); }

set_rhs = value %observe_set_rhs_value space*
    (
          ('+' space* value) %observe_set_rhs_plus_value
        | ('-' space* value) %observe_set_rhs_minus_value
    )? space*;
set_action = space* path %observe_set_action_path space*
    '=' space* set_rhs %observe_set_action;
remove_action = space* path %observe_remove_action space*;
add_action = space* (path space+ valref) %observe_add_action space*;
delete_action = space* (path space+ valref) %observe_delete_action space*;

expression_clause = (
      (/SET/i set_action (',' set_action)*)
    | (/ADD/i add_action (',' add_action)*)
    | (/REMOVE/i remove_action (',' remove_action)*)
    | (/DELETE/i delete_action (',' delete_action)*)
) %observe_expression_clause;


# Left-Guarded Concatenation used here because space after first value in set_rhs can also start
# matching next expression_clause and as a result would cause nondeterminism leading to state
# expansion bomb.
main := expression_clause <: expression_clause**;
}%%

%%{
machine condition_parser;
include base "expressions_base.rl";
access _fsm_;

action observe_pr_cond_value { observe_pr_cond_value(); }
action observe_pr_cond_between { observe_pr_cond_op(op_t::BETWEEN); }
action observe_pr_cond_in { observe_pr_cond_op(op_t::IN); }
action xxx { xxx(); }

# This start action is needed because condition_expression is a recursive type (can contain other conditions).
# It will be used to handle stack growth and has to be executed on start rather than end transition.
action observe_cond_start { observe_cond_start(); }

comparison = (
      '=' %{ observe_pr_cond_op(op_t::EQ); }
    | ('<' '>') %{ observe_pr_cond_op(op_t::NE); }
    | '<' %{ observe_pr_cond_op(op_t::LT); }
    | ('<' '=') %{ observe_pr_cond_op(op_t::LE); }
    | '>' %{ observe_pr_cond_op(op_t::GT); }
    | ('>' '=') %{ observe_pr_cond_op(op_t::GE); }
);

primitive_condition = value %observe_pr_cond_value space* (
      (comparison space* value %observe_pr_cond_value)
    | (/BETWEEN/i space+ value %observe_pr_cond_value
           /AND/i space+ value %observe_pr_cond_value) %observe_pr_cond_between
    | (/IN/i space* '(' space* value %observe_pr_cond_value space*
            (',' space* value %observe_pr_cond_value)* space* ')') %observe_pr_cond_in
)?;

condition_expression_3a := primitive_condition;

#condition_expression_3a := (
#      primitive_condition
#    | '(' @{ fcall condition_expression_3_after_parenthesis; }
#);

condition_expression_3 = 'FEXO';

# Left-Guarded Concatenation used here because NOT can also match value name but NOT operator has precedence.
condition_expression_2 = (/NOT/i space*)? <: condition_expression_3;

# condition_expression_2 = 'FEFE';


condition_expression_1 = condition_expression_2 space* (/AND/i space* condition_expression_2)*;
condition_expression = condition_expression_1 space* (/OR/i space* condition_expression_1)*;

condition_expression_3_after_parenthesis := (
    space* condition_expression space* ')' @{ fret; }
);

main := condition_expression;
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

    [[gnu::always_inline]]
    void init(std::string_view& buf) {
        p = buf.data();
        pe = p + buf.size();
        eof = pe;
        _cur_start = _cur_end = p;
    }

    [[gnu::always_inline]]
    void throw_on_error(int first_final_state) {
        if (_fsm_cs < first_final_state) {
            throw expressions_syntax_error(format("Parse error after position {}", eof - _cur_start));
        }
        if (p < pe) {
            // ended in final state but some data left to read
            throw expressions_syntax_error(format("Parse error after position {}", eof - _cur_start));
        }
    }

    // Ragel requires all action functions to be defined even if not used (no transition in FSM
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
        return std::exchange(_cur_path, parsed::path());
    }

    [[gnu::always_inline]]
    void init() {
        _cur_path = parsed::path();
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
        std::cout << _value_stack.size() << std::endl;
        assert(_value_stack.size() == 1);
        auto v = std::move(_value_stack[0]);
        _value_stack.clear();
        return v;
    }

    [[gnu::always_inline]]
    void init() {
        _value_stack.clear();
        _cur_value = nullptr;
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
        parser_path_handler::init();
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
    parsed::update_expression _cur_exp;
    // Used only for SET action, others can be constructed directly in _cur_exp.
    parsed::action _cur_action; 
    parsed::set_rhs _cur_rhs;
public:
    parsed::update_expression parse(std::string_view buf) {
        %% write init;
        parser_base::init(buf);
        parser_path_handler::init();
        parser_value_handler::init();
        update_parser::init();
        %% write exec;
        throw_on_error(%%{ write first_final; }%%);
        return _res;
    }
private:
    [[gnu::always_inline]]
    void observe_set_rhs_value() {
        std::cout << "observe_set_rhs_value" << std::endl;
        _cur_rhs.set_value(move_cur_value());
    }

    [[gnu::always_inline]]
    void observe_set_rhs_plus_value() {
        std::cout << "observe_set_rhs_plus_value" << std::endl;
        _cur_rhs.set_plus(move_cur_value());
    }

    [[gnu::always_inline]]
    void observe_set_rhs_minus_value() {
        std::cout << "observe_set_rhs_minus_value" << std::endl;
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
        std::cout << "observe_set_action" << std::endl;
        auto a = move_cur_action();
        a.assign_set_rhs(move_cur_rhs());
        _cur_exp.add(std::move(a));
    }
    
    [[gnu::always_inline]]
    void observe_remove_action() {
        parsed::action a;
        a.assign_remove(move_cur_path());
        _cur_exp.add(std::move(a));
    }
    
    [[gnu::always_inline]]
    void observe_add_action() {
        parsed::action a;
        a.assign_add(move_cur_path(), str());
        _cur_exp.add(std::move(a));
    }
    
    [[gnu::always_inline]]
    void observe_delete_action() {
        parsed::action a;
        a.assign_del(move_cur_path(), str());
        _cur_exp.add(std::move(a));
    }

    [[gnu::always_inline]]
    void observe_expression_clause() {
        std::cout << "observe_expression_clause" << std::endl;
        _res.append(std::move(_cur_exp));
        _cur_exp = parsed::update_expression();
    }

    [[gnu::always_inline]]
    void init() {
        _res = parsed::update_expression();
        _cur_exp = parsed::update_expression();
        _cur_action = parsed::action();
        _cur_rhs = parsed::set_rhs();
    }
};

class condition_parser :
private virtual parser_base,
    private virtual parser_path_handler,
    private virtual parser_value_handler
{
    %% machine condition_parser;
    %% write data;

    using op_t = parsed::primitive_condition::type;
    parsed::primitive_condition _cur_pr_cond;

    std::vector<parsed::condition_expression> _cond_stack;
    parsed::condition_expression* _cur_cond; // Points to top element from the stack.
public:
    parsed::condition_expression parse(std::string_view buf) {
        %% write init;
        parser_base::init(buf);
        parser_path_handler::init();
        parser_value_handler::init();
        condition_parser::init();
        %% write exec;
        throw_on_error(%%{ write first_final; }%%);
        return _res;
    }
private:
    [[gnu::always_inline]]
    void observe_pr_cond_op(op_t op) {
        _cur_pr_cond.set_operator(op);
    }

    [[gnu::always_inline]]
    void observe_pr_cond_value() {
        _cur_pr_cond.add_value(move_cur_value());
    }

    [[gnu::always_inline]]
    parsed::primitive_condition move_cur_pr_cond() {
        return std::exchange(_cur_pr_cond, parsed::primitive_condition());
    }

    [[gnu::always_inline]]
    void init() {
        //_res = parsed::condition_expression();
        // TODO!!!!!
    }
};


} // namespace alternator