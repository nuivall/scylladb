#pragma once

#include <vector>
#include <string_view>

#include "alternator/expressions_types.hh"
#include "alternator/expressions.hh"

%%{
machine projection_parser;
include base "expressions_base.rl";
access _fsm_;

# embeding spaces directly inside path would have bad effect on matching
# other states which embed path but not consider space as the beginning of path matching
spaced_path = space* path space* ;
main := spaced_path (',' spaced_path)* ;
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

    void observe_path_root() {}
    void observe_path_dot() {}
    void observe_path_index() {}

    void observe_value_valref() {}
    void observe_value_path() {}
    void observe_value_func_name() {}
    void observe_value_func_call() {}
    void observe_value_func_param() {}
    void observe_value_start() {}
};

class projection_parser : public parser_base {
    %% machine projection_parser;
    %% write data noerror nofinal noprefix;
    std::vector<parsed::path> _res;
public:
    std::vector<parsed::path> parse(std::string_view buf) {
        %% write init;
        init(buf);
        _res.clear();
        %% write exec;
        throw_on_error(%%{ write first_final; }%%);
        return _res;
    }

private:
    [[gnu::always_inline]]
    void observe_path_root() {
        _res.emplace_back(str());
    }

    [[gnu::always_inline]]
    void observe_path_dot() {
        _res.back().add_dot(str());
    }

    [[gnu::always_inline]]
    void observe_path_index() {
        _res.back().add_index(std::stoi(str()));
    }
};


%%{
machine value_parser;
include base "expressions_base.rl";
access _fsm_;

main := value;
}%%

class value_parser {
    %% machine value_parser;
    // static data for generated code, no* options disable not needed stuff
    %% write data noerror nofinal noprefix;

    int _fsm_cs;
    // char* _fsm_ts;
    // char* _fsm_te;

    // pointers to start and end of current token match
    // char* _fsm_tokstart = nullptr;
    // char* _fsm_tokend = nullptr;
    // int _fsm_act = 0; // identity of the last pattern matched, used for backtracking
    static constexpr int _nesting_limit = 32;
    int _fsm_stack[_nesting_limit];
    int _fsm_top;

    std::vector<parsed::value> _res;

    parsed::value* _cur_value;
    parsed::path _cur_path;

    // pointers to start and end of current "token" match
    // it's used in FSM by calls to mark_start and mark_end
    const char* _cur_start;
    const char* _cur_end;

public:
    parsed::value parse(std::string_view buf) {
        %% write init;
        _res.clear(); // parse should be safe for reuse

        // needed as an interface with generated code
        const char* p = buf.data();
        const char* pe = p + buf.size();
        const char* eof = pe;

        _cur_start = _cur_end = p;
        
        %% write exec;

        if (_fsm_cs < %%{ write first_final; }%%) {
            throw expressions_syntax_error(format("Parse error after position {}",  eof - _cur_start));
        }
        if (p < pe) {
            // ended in final state but some data left to read
            throw expressions_syntax_error(format("Parse error after position {}", eof - _cur_start));
        }

        return std::move(_res[0]);
    }

private:
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
    void observe_value_start() {
        _res.push_back(parsed::value());
        _cur_value = &_res.back();
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
    void observe_value_func_call() {
        //_cur_value->set_valref(str());
    }

    [[gnu::always_inline]]
    void observe_value_func_param() {
        _cur_value = &_res[_res.size() - 2];
        _cur_value->add_func_parameter(std::move(_res.back()));
        _res.pop_back();
    }

    // void observe_valref() {}
    // void observe_path() {}
    // void observe_func_call() {}
    // void observe_value() {}

};

} // namespace alternator