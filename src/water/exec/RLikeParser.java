/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.exec;

import java.util.Arrays;
import water.Key;
import water.Stream;

/**
 * 
 * 
 * 
 * 
 * 
 * Grammar:
 * 
 * S  -> T S1
 * S1 -> e
 *       + T S1
 *       - T S1
 * T  -> number 
 *
 * @author peta
 */
public class RLikeParser {
  
  // ---------------------------------------------------------------------------
  // Lexer part 
  //
  // A simple lexer that can support a LL(1) grammar for the time being.
  // We might get more complicated here in the future. But for now, I can't care
  // less.
  // ---------------------------------------------------------------------------
  
  public static class Token {
    public enum Type {
      ttNumber, // any number
      ttIdent, // any identifier
      ttOpAssign, // assignment
      ttOpAdd, // +
      ttOpSub, // -
      ttOpMul, // *
      ttOpDiv, // /
      ttOpParOpen, // (
      ttOpParClose, // )
      ttOpDoubleQuote, // "
      ttEOF,
      ttUnknown,
      ;
      public String toString() {
        switch (this) {
          case ttNumber:
            return "number";
          case ttIdent:
            return "identifier";
          case ttOpAssign:
            return "assignment";
          case ttOpAdd:
            return "operator +";
          case ttOpSub:
            return "operator -";
          case ttOpMul:
            return "operator *";
          case ttOpDiv:
            return "operator /";
          case ttOpParOpen:
            return "opening parenthesis";
          case ttOpParClose:
            return "closing parenthesis";
          case ttOpDoubleQuote:
            return "\"";
          case ttEOF:
            return "end of input";
          default:
            return "unknown token";
        }
      }
    }
    public final Type type;
    public final double value;
    public final String id;
    
    public Token(Type type) {
      this.type = type;
      value = 0;
      id = "";
    }
    
    public Token(double d) {
      this.type = Type.ttNumber;
      value = d;
      id = "";
    }
    
    public Token(String s) {
      this.type = Type.ttIdent;
      value = 0;
      id = s;
    }
  }
 
  private Stream s_;
  
  private Token top_;
  
  protected Token top() {
    return top_;  
  }
  
  protected Token pop() {
    Token x = top_;
    top_ = parseNextToken();
    return x;
  }

  protected Token pop(Token.Type type) {
    if (top().type != type)
      throw new RuntimeException("Token "+type.toString()+" expected, but "+top().type.toString()+" found.");
    return pop();
  }
  
  private void skipWhitespace() {
    char c;
    while (true) {
      if (s_.eof())
        break;
      c = (char) s_.peek1();
      if ((c == ' ') || (c == '\t') || (c == '\n')) {
        s_.get1();
        continue;
      }
      break;
    }
  }
  
  private boolean isCharacter(char c) {
    if ((c >= 'a') && (c <= 'z'))
      return true;
    if ((c >= 'A') && (c <= 'Z'))
      return true;
    return c == '_';
  }
  
  private boolean isDigit(char c) {
    return (c>='0') && (c <='9');
  }
  
  private Token parseNextToken() {
    skipWhitespace();
    if (s_.eof())
      return new Token(Token.Type.ttEOF);
    char c = (char) s_.peek1();
    switch (c) {
      case '=':
        ++s_._off;
        return new Token(Token.Type.ttOpAssign);
      case '+':
        ++s_._off;
        return new Token(Token.Type.ttOpAdd);
      case '-':
        ++s_._off;
        return new Token(Token.Type.ttOpSub);
      case '*':
        ++s_._off;
        return new Token(Token.Type.ttOpMul);
      case '/':
        ++s_._off;
        return new Token(Token.Type.ttOpDiv);
      case '(':
        ++s_._off;
        return new Token(Token.Type.ttOpParOpen);
      case ')':
        ++s_._off;
        return new Token(Token.Type.ttOpParClose);
      case '"':
        ++s_._off;
        return new Token(Token.Type.ttOpDoubleQuote);
      default:
        if (isCharacter(c)) 
          return parseIdent();
        if (isDigit(c))
          return parseNumber();
    }
    return new Token(Token.Type.ttUnknown);
  }

  private Token parseIdent() {
    int start = s_._off;
    while (true) {
      if (s_.eof())
        break;
      char c = (char) s_.peek1();
      if (isCharacter(c) || isDigit(c) || (c=='.')) {
        ++s_._off;
        continue;
      }
      break;
    }
    return new Token(new String(s_._buf, start, s_._off - start));
  }
  
  private Token parseNumber() {
    int start = s_._off;
    boolean dot = false;
    boolean e = false;
    while (true) {
      if (s_.eof())
        break;
      char c = (char) s_.peek1();
      if (isDigit(c)) {
        ++s_._off;
        continue;
      }
      if (c == '.') {
        if (dot != false)
          throw new RuntimeException("Only one dot can be present in number.");
        dot = true;
        ++s_._off;
        continue;
      }
      if ((c == 'e') || (c == 'E')) {
        if (e != false)
          throw new RuntimeException("Only one exponent can be present in number.");
        e = true;
        ++s_._off;
        continue;
      }
      break;
    }
    return new Token(Double.parseDouble(new String(s_._buf, start, s_._off - start)));
  }
  
  // ---------------------------------------------------------------------------
  // Parser part
  //
  // A simple LL(1) recursive descent guy. With the following grammar:

  public Expr parse(String x) {
    return parse(new Stream(x.getBytes()));
  }
  
  public Expr parse(Stream x) {
    s_ = x;
    pop(); // load the first token in the stream
    if (top().type == Token.Type.ttOpDoubleQuote) {
      pop(); 
      Expr result = parse_S();
      pop(Token.Type.ttOpDoubleQuote);
      pop(Token.Type.ttEOF); // make sure we have parsed everything
      return result;
    } else {
      Expr result = parse_S();
      pop(Token.Type.ttEOF); // make sure we have parsed everything
      return result;
    }
  }
  
  
  /** Parses the expression in R. 
   * 
   * S -> e | T { + T | - T }
   * 
   * @return 
   */
  private Expr parse_S() {
    if (top().type == Token.Type.ttEOF)
      return null;
    Expr result = parse_T();
    while ((top().type == Token.Type.ttOpAdd) || (top().type == Token.Type.ttOpSub)) {
      Token.Type type = pop().type;
      result = new BinaryOperator(type,result,parse_T());
    }
    return result;
  }
  
  /*
   * T -> F { * F | / F }
   */
  private Expr parse_T() {
    Expr result = parse_F();
    while ((top().type == Token.Type.ttOpMul) || (top().type == Token.Type.ttOpDiv)) {
      Token.Type type = pop().type;
      result = new BinaryOperator(type,result,parse_T());
    }
    return result;
  }
  
  /*
   * F -> number | ident { = S } | ( S ) 
   */
  
  private Expr parse_F() {
    switch (top().type) {
      case ttNumber:
        return new FloatLiteral(pop().value);
      case ttIdent: {
        Token t = pop();
        if (top().type == Token.Type.ttOpAssign) {
          pop();
          Expr rhs = parse_S();
          return new AssignmentOperator(Key.make(t.id), rhs);
        } else {
          return new KeyLiteral(t.id);
        }
      }
      case ttOpParOpen: {
        pop();
        Expr e = parse_S();
        pop(Token.Type.ttOpParClose);
        return e;
      }
      default:
        throw new RuntimeException("Number or parenthesis expected, but "+top().type.toString()+" found.");
    }
  }
  
}
