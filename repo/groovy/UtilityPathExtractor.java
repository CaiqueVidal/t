package com.exemplo.groovy;

import org.codehaus.groovy.ast.ASTNode;
import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.ast.CodeVisitorSupport;
import org.codehaus.groovy.ast.ConstructorNode;
import org.codehaus.groovy.ast.FieldNode;
import org.codehaus.groovy.ast.MethodNode;
import org.codehaus.groovy.ast.builder.AstBuilder;
import org.codehaus.groovy.ast.expr.BinaryExpression;
import org.codehaus.groovy.ast.expr.ConstantExpression;
import org.codehaus.groovy.ast.expr.DeclarationExpression;
import org.codehaus.groovy.ast.expr.Expression;
import org.codehaus.groovy.ast.expr.GStringExpression;
import org.codehaus.groovy.ast.expr.MethodCallExpression;
import org.codehaus.groovy.ast.expr.PropertyExpression;
import org.codehaus.groovy.ast.expr.VariableExpression;
import org.codehaus.groovy.ast.stmt.BlockStatement;
import org.codehaus.groovy.ast.stmt.Statement;
import org.codehaus.groovy.control.CompilePhase;
import org.codehaus.groovy.syntax.Types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class UtilityPathExtractor {

    public List<UtilyUse> extract(List<String> utilities, String groovyScript) {
        Set<String> utilitySet = Set.copyOf(utilities);

        Map<String, Set<String>> usagesByName = new LinkedHashMap<>();
        for (String utility : utilities) {
            usagesByName.put(utility, new TreeSet<>());
        }

        Map<String, String> variableMap = new HashMap<>();

        List<ASTNode> nodes = new AstBuilder().buildFromString(
                CompilePhase.CONVERSION,
                false,
                groovyScript
        );

        CodeVisitorSupport visitor = new CodeVisitorSupport() {
            @Override
            public void visitDeclarationExpression(DeclarationExpression expression) {
                super.visitDeclarationExpression(expression);

                if (expression.getLeftExpression() instanceof VariableExpression leftVar) {
                    String path = extractPath(expression.getRightExpression(), variableMap);
                    if (path != null) {
                        variableMap.put(leftVar.getName(), path);
                        registerUsage(path, utilitySet, usagesByName);
                    }
                }
            }

            @Override
            public void visitBinaryExpression(BinaryExpression expression) {
                super.visitBinaryExpression(expression);

                if (expression.getOperation() == null) {
                    return;
                }

                int operationType = expression.getOperation().getType();

                // Assignment: x = ...
                if (operationType == Types.ASSIGN
                        && expression.getLeftExpression() instanceof VariableExpression leftVar) {

                    String path = extractPath(expression.getRightExpression(), variableMap);
                    if (path != null) {
                        variableMap.put(leftVar.getName(), path);
                        registerUsage(path, utilitySet, usagesByName);
                    }
                    return;
                }

                // Index access: foo["code"], foo[0], alias["code"], alias[0]
                if (operationType == Types.LEFT_SQUARE_BRACKET) {
                    String path = extractPath(expression, variableMap);
                    if (path != null) {
                        registerUsage(path, utilitySet, usagesByName);
                    }
                }
            }

            @Override
            public void visitPropertyExpression(PropertyExpression expression) {
                super.visitPropertyExpression(expression);

                String path = extractPath(expression, variableMap);
                if (path != null) {
                    registerUsage(path, utilitySet, usagesByName);
                }
            }

            @Override
            public void visitMethodCallExpression(MethodCallExpression call) {
                super.visitMethodCallExpression(call);

                String target = extractPath(call.getObjectExpression(), variableMap);
                if (target != null && call.getMethodAsString() != null) {
                    registerUsage(target + "." + call.getMethodAsString(), utilitySet, usagesByName);
                }
            }

            @Override
            public void visitGStringExpression(GStringExpression expression) {
                super.visitGStringExpression(expression);

                for (Expression value : expression.getValues()) {
                    String path = extractPath(value, variableMap);
                    if (path != null) {
                        registerUsage(path, utilitySet, usagesByName);
                    }
                }
            }
        };

        for (ASTNode node : nodes) {
            visitRootNode(node, visitor);
        }

        List<UtilyUse> result = new ArrayList<>();
        for (String utility : utilities) {
            List<String> paths = new ArrayList<>(usagesByName.getOrDefault(utility, Collections.emptySet()));
            result.add(new UtilyUse(utility, paths));
        }

        return result;
    }

    private static void visitRootNode(ASTNode node, CodeVisitorSupport visitor) {
        if (node == null) {
            return;
        }

        if (node instanceof BlockStatement block) {
            block.visit(visitor);
            return;
        }

        if (node instanceof Statement statement) {
            statement.visit(visitor);
            return;
        }

        if (node instanceof ClassNode classNode) {
            visitClassNode(classNode, visitor);
            return;
        }

        if (node instanceof MethodNode methodNode) {
            if (methodNode.getCode() != null) {
                methodNode.getCode().visit(visitor);
            }
            return;
        }

        if (node instanceof ConstructorNode constructorNode) {
            if (constructorNode.getCode() != null) {
                constructorNode.getCode().visit(visitor);
            }
        }
    }

    private static void visitClassNode(ClassNode classNode, CodeVisitorSupport visitor) {
        if (classNode == null) {
            return;
        }

        for (MethodNode method : classNode.getMethods()) {
            if (method.getCode() != null) {
                method.getCode().visit(visitor);
            }
        }

        for (ConstructorNode constructor : classNode.getDeclaredConstructors()) {
            if (constructor.getCode() != null) {
                constructor.getCode().visit(visitor);
            }
        }

        for (Statement statement : classNode.getObjectInitializerStatements()) {
            if (statement != null) {
                statement.visit(visitor);
            }
        }

        for (FieldNode field : classNode.getFields()) {
            if (field.getInitialExpression() != null) {
                field.getInitialExpression().visit(visitor);
            }
        }
    }

    private static void registerUsage(
            String path,
            Set<String> utilitySet,
            Map<String, Set<String>> usagesByName
    ) {
        String root = extractRoot(path);
        if (root != null && utilitySet.contains(root)) {
            usagesByName.get(root).add(path);
        }
    }

    private static String extractRoot(String path) {
        if (path == null || path.isBlank()) {
            return null;
        }

        int dotIndex = path.indexOf('.');
        int bracketIndex = path.indexOf('[');

        if (dotIndex == -1 && bracketIndex == -1) {
            return path;
        }
        if (dotIndex == -1) {
            return path.substring(0, bracketIndex);
        }
        if (bracketIndex == -1) {
            return path.substring(0, dotIndex);
        }

        return path.substring(0, Math.min(dotIndex, bracketIndex));
    }

    private static String extractPath(Expression expression, Map<String, String> variableMap) {
        if (expression == null) {
            return null;
        }

        // Direct alias: x
        if (expression instanceof VariableExpression variableExpression) {
            return variableMap.get(variableExpression.getName());
        }

        // contexto.$utilitarios.foo.payload / contexto.$utilitarios.foo["code"] / contexto.$utilitarios.foo[0]
        String directPath = extractContextUtilityPath(expression);
        if (directPath != null) {
            return directPath;
        }

        // Alias with property: x.total
        if (expression instanceof PropertyExpression propertyExpression) {
            String base = extractPath(propertyExpression.getObjectExpression(), variableMap);
            String property = propertyExpression.getPropertyAsString();

            if (base != null && property != null) {
                return base + "." + property;
            }
        }

        // Alias with index: x["code"], x[0]
        if (expression instanceof BinaryExpression binaryExpression
                && binaryExpression.getOperation() != null
                && binaryExpression.getOperation().getType() == Types.LEFT_SQUARE_BRACKET) {

            String base = extractPath(binaryExpression.getLeftExpression(), variableMap);
            String index = extractIndex(binaryExpression.getRightExpression(), variableMap);

            if (base != null && index != null) {
                if (isQuotedStringIndex(index)) {
                    return base + "." + unquote(index);
                }
                return base + "[" + index + "]";
            }
        }

        // Method call on alias: x.length()
        if (expression instanceof MethodCallExpression methodCallExpression) {
            String base = extractPath(methodCallExpression.getObjectExpression(), variableMap);
            if (base != null && methodCallExpression.getMethodAsString() != null) {
                return base + "." + methodCallExpression.getMethodAsString();
            }
        }

        // GString / interpolation
        if (expression instanceof GStringExpression gStringExpression) {
            for (Expression value : gStringExpression.getValues()) {
                String path = extractPath(value, variableMap);
                if (path != null) {
                    return path;
                }
            }
        }

        return null;
    }

    private static String extractContextUtilityPath(Expression expression) {
        List<String> parts = new ArrayList<>();

        Expression current = expression;

        while (true) {
            if (current instanceof PropertyExpression propertyExpression) {
                String property = propertyExpression.getPropertyAsString();
                if (property == null) {
                    return null;
                }
                parts.add(0, property);
                current = propertyExpression.getObjectExpression();
                continue;
            }

            // Handles index access: foo["code"], foo[0]
            if (current instanceof BinaryExpression binaryExpression
                    && binaryExpression.getOperation() != null
                    && binaryExpression.getOperation().getType() == Types.LEFT_SQUARE_BRACKET) {

                String index = extractIndex(binaryExpression.getRightExpression(), Collections.emptyMap());
                if (index == null) {
                    return null;
                }

                if (isQuotedStringIndex(index)) {
                    parts.add(0, unquote(index));
                } else {
                    parts.add(0, "[" + index + "]");
                }

                current = binaryExpression.getLeftExpression();
                continue;
            }

            break;
        }

        if (!(current instanceof VariableExpression variableExpression)) {
            return null;
        }

        if (!"contexto".equals(variableExpression.getName())) {
            return null;
        }

        if (parts.size() < 2) {
            return null;
        }

        if (!"$utilitarios".equals(parts.get(0))) {
            return null;
        }

        // Removes "$utilitarios"
        List<String> pathParts = parts.subList(1, parts.size());

        if (pathParts.isEmpty()) {
            return null;
        }

        return joinPathParts(pathParts);
    }

    private static String extractIndex(Expression expression, Map<String, String> variableMap) {
        if (expression instanceof ConstantExpression constantExpression) {
            Object value = constantExpression.getValue();
            if (value instanceof String stringValue) {
                return "\"" + stringValue + "\"";
            }
            return String.valueOf(value);
        }

        String alias = extractPath(expression, variableMap);
        if (alias != null) {
            return alias;
        }

        return null;
    }

    private static boolean isQuotedStringIndex(String value) {
        return value != null
                && value.length() >= 2
                && value.startsWith("\"")
                && value.endsWith("\"");
    }

    private static String unquote(String value) {
        return value.substring(1, value.length() - 1);
    }

    private static String joinPathParts(List<String> parts) {
        StringBuilder sb = new StringBuilder();

        for (String part : parts) {
            if (part.startsWith("[")) {
                sb.append(part);
            } else {
                if (!sb.isEmpty()) {
                    sb.append('.');
                }
                sb.append(part);
            }
        }

        return sb.toString();
    }
}