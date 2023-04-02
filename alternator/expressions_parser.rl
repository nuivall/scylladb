#pragma once

#include <vector>
#include <string_view>

#include "alternator/expressions_types.hh"
#include "alternator/expressions.hh"

%%{
machine projection_parser;
include base "expressions_base.rl";
access _fsm_;

main := path (',' path)* ;
}%%

namespace alternator {

class projection_parser {
    %% machine projection_parser;
    // static data for generated code, no* options disable not needed stuff
    %% write data noerror nofinal noprefix;

    int _fsm_cs;
    // char* _fsm_ts;
    // char* _fsm_te;

    // pointers to start and end of current token match
    // char* _fsm_tokstart = nullptr;
    // char* _fsm_tokend = nullptr;
    // int _fsm_act = 0; // identity of the last pattern matched, used for backtracking

     std::vector<parsed::path> _res;

    // pointers to start and end of current "token" match
    // it's flexibly used in FSM by calls to mark_start and mark_end
    const char* _cur_start;
    const char* _cur_end;

public:
    std::vector<parsed::path> parse(std::string_view buf) {
        %% write init;
        _res.resize(0);

        // needed as an interface with generated code
        const char* p = buf.data();
        const char* pe = p + buf.size();
        const char* eof = pe;

        _cur_start = _cur_end = p;
        
        %% write exec;

        if (_fsm_cs < %%{ write first_final; }%%) {
            throw expressions_syntax_error(format("xParse error after position {}",  buf.data() + buf.size() - _cur_start));
        }
        if (p < pe) {
            // ended in final state but some data left to read
            throw expressions_syntax_error(format("Parse error after position {}", eof - _cur_start));
        }

        return _res;
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

} // namespace alternator