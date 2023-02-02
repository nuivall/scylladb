/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <antlr3.hpp>
#include <string>
#include <stdexcept>
#include <vector>
#include <unordered_set>
#include <string_view>

#include <seastar/util/noncopyable_function.hh>

#include "expressions_types.hh"
#include "utils/rjson.hh"

namespace alternator {

class expressions_syntax_error : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

parsed::update_expression parse_update_expression(std::string_view query);
std::vector<parsed::path> parse_projection_expression(std::string_view query);
parsed::condition_expression parse_condition_expression(std::string_view query);

void resolve_update_expression(parsed::update_expression& ue,
        const rjson::value* expression_attribute_names,
        const rjson::value* expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& used_attribute_values);
void resolve_projection_expression(std::vector<parsed::path>& pe,
        const rjson::value* expression_attribute_names,
        std::unordered_set<std::string>& used_attribute_names);
void resolve_condition_expression(parsed::condition_expression& ce,
        const rjson::value* expression_attribute_names,
        const rjson::value* expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& used_attribute_values);

void validate_value(const rjson::value& v, const char* caller);

bool condition_expression_on(const parsed::condition_expression& ce, std::string_view attribute);

// for_condition_expression_on() runs the given function on the attributes
// that the expression uses. It may run for the same attribute more than once
// if the same attribute is used more than once in the expression.
void for_condition_expression_on(const parsed::condition_expression& ce, const noncopyable_function<void(std::string_view)>& func);

// calculate_value() behaves slightly different (especially, different
// functions supported) when used in different types of expressions, as
// enumerated in this enum:
enum class calculate_value_caller {
    UpdateExpression, ConditionExpression, ConditionExpressionAlone
};

inline std::ostream& operator<<(std::ostream& out, calculate_value_caller caller) {
    switch (caller) {
        case calculate_value_caller::UpdateExpression:
            out << "UpdateExpression";
            break;
        case calculate_value_caller::ConditionExpression:
            out << "ConditionExpression";
            break;
        case calculate_value_caller::ConditionExpressionAlone:
            out << "ConditionExpression";
            break;
        default:
            out << "unknown type of expression";
            break;
    }
    return out;
}

rjson::value calculate_value(const parsed::value& v,
        calculate_value_caller caller,
        const rjson::value* previous_item);

rjson::value calculate_value(const parsed::set_rhs& rhs,
        const rjson::value* previous_item);


// TODO: needs to be changed
	enum Tokens 
	{
		EOF_TOKEN = (ANTLR_CHARSTREAM_EOF & 0xFFFFFFFF)
		, T__51 = 51 
		, T__52 = 52 
		, T__53 = 53 
		, T__54 = 54 
		, T__55 = 55 
		, T__56 = 56 
		, T__57 = 57 
		, A = 4 
		, ADD = 5 
		, ALNUM = 6 
		, ALPHA = 7 
		, AND = 8 
		, B = 9 
		, BETWEEN = 10 
		, C = 11 
		, CLOSE_BRACKET = 12 
		, COMMA = 13 
		, D = 14 
		, DELETE = 15 
		, DIGIT = 16 
		, E = 17 
		, EQ = 18 
		, F = 19 
		, G = 20 
		, H = 21 
		, I = 22 
		, IN = 23 
		, INTEGER = 24 
		, J = 25 
		, K = 26 
		, L = 27 
		, M = 28 
		, N = 29 
		, NAME = 30 
		, NAMEREF = 31 
		, NOT = 32 
		, O = 33 
		, OPEN_BRACKET = 34 
		, OR = 35 
		, P = 36 
		, Q = 37 
		, R = 38 
		, REMOVE = 39 
		, S = 40 
		, SET = 41 
		, T = 42 
		, U = 43 
		, V = 44 
		, VALREF = 45 
		, W = 46 
		, WHITESPACE = 47 
		, X = 48 
		, Y = 49 
		, Z = 50 
	};

template<class ImplTraits>
class customInputStream /* : public ImplTraits::template IntStreamType< typename ImplTraits::InputStreamType >*/ {
    public:
    	typedef typename ImplTraits::template IntStreamType< typename ImplTraits::InputStreamType > IntStreamType;
        typedef typename ImplTraits::StreamDataType UnitType;
        typedef typename ImplTraits::StringType StringType;
        typedef UnitType DataType;

        StringType substr(ANTLR_MARKER start, ANTLR_MARKER stop) {
            return "";
         }

        const DataType* get_data() const {
            return nullptr;
        }

        const StringType& get_fileName() const {
            return "";
        }

        // const DataType* get_nextChar() const {
        //     throw "not used";
        // }

        customInputStream() {};
};

template<class ImplTraits>
class customTokenStream /*: public antlr3::CommonTokenStream<ImplTraits>*/ {
public:
        typedef typename ImplTraits::CommonTokenType TokenType;
        typedef typename ImplTraits::TokenIntStreamType IntStreamType;
        typedef TokenType UnitType;
        typedef typename ImplTraits::StringType StringType;
        //typedef typename ImplTraits::TokenSourceType TokenSourceType;
        typedef std::string_view TokenSourceType;

        typedef typename ImplTraits::InputStreamType InputStreamType;

        customTokenStream(ANTLR_UINT32, TokenSourceType source): _source(source) {
            // TODO: rewrite
            size_t j = 0;
            auto size = _source.size();
            for(size_t i=0; i <= size;i++) {
                if (i == size || _source[i]==' ') {
                    if (j==i) {
                        j = i+1;
                        continue;
                    }
                    // tokenize
                    auto v = _source.substr(j, i-j);
                    /*
                    Recognize:
                        VALREF
                        IN
                        AND
                        CLOSE_BRACKET
                        OPEN_BRACKET
                        COMMA
                        NAME
                        EQ
                    */
                    ANTLR_UINT32 typ = 0;
                    if (v == "IN") {
                        typ = IN;
                    } else if (v == "AND") {
                        typ = AND;
                    } else if (v.size() == 1) {
                        switch (v[0]) {
                            case '(':
                                typ = OPEN_BRACKET;
                                break;
                            case ')':
                                typ = CLOSE_BRACKET;
                                break;
                            case ',':
                                typ = COMMA;
                                break;
                            case '=':
                                typ = EQ;
                                break;
                            default:
                                 // TODO: number or letter
                                typ = NAME;
                                break;
                        }
                    } else {
                        if (v[0] == ':') {
                            typ = VALREF;
                        } else {
                            // TODO: add others
                            typ = NAME;
                        }
                    }
                   
                    auto tok = tokenType(typ);
                    tok.set_input(&dummy);
                    _tokens.push_back(std::move(tok));
                    j = i+1;
                }
            }
            _tokens.emplace_back(Tokens::EOF_TOKEN);
        }

    	// const TokenType* LB(ANTLR_INT32 k) {
        //     return nullptr;
        // }

        void consume() {
            _pos++;
        }

        ANTLR_MARKER  index() const {
            return _pos;
        }

        ANTLR_UINT32 _LA(ANTLR_INT32 i) {
            if (i > 0) i--;
            return _tokens.at(_pos+i).getType();
        }

        const TokenType* _LT(ANTLR_INT32 k) {
            if (k > 0) k--;
            return &_tokens.at(_pos+k);
        }

        StringType toStringTT(const TokenType* start, const TokenType* stop) {
            StringType str;
            for (auto pos = start->get_index(); pos <= stop->get_index(); pos++) {
                auto& tok = _tokens[pos];
                auto start = tok.get_startIndex();
                auto stop = tok.get_startIndex();
                str += _source.substr(start, stop-start);
            }
            return str;
        }

        private:
            using tokenType = antlr3::CommonToken<ImplTraits>;

            std::string_view _source;
            std::vector<tokenType> _tokens;
            size_t _pos = 0;

            InputStreamType dummy;
};


} /* namespace alternator */
